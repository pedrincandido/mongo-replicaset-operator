#!/usr/bin/env bash
# End-to-end backup test: a backup only counts once it has been restored.
#
#   1. write N documents
#   2. run the backup CronJob once, now
#   3. drop the database
#   4. restore the newest archive with a one-off Job
#   5. check every document came back
#
# Usage: hack/backup-restore-test.sh [name] [namespace]
set -euo pipefail

NAME=${1:-demo}
NS=${2:-default}
N=500
DB=restoretest
STAMP=$(date +%s)

hosts=""
members=$(kubectl -n "$NS" get mrs "$NAME" -o jsonpath='{.spec.members}')
for i in $(seq 0 $((members - 1))); do
  hosts+="${NAME}-${i}.${NAME}.${NS}.svc.cluster.local:27017,"
done
URI="mongodb://${hosts%,}/?replicaSet=${NAME}"

mongo() { kubectl -n "$NS" exec "${NAME}-0" -c mongod -- mongosh --quiet --norc "$URI" --eval "$1"; }

echo "1. writing $N documents to $DB.items"
mongo "db.getSiblingDB('$DB').dropDatabase(); const docs = []; for (let i = 0; i < $N; i++) docs.push({ i, at: new Date() }); db.getSiblingDB('$DB').items.insertMany(docs, { writeConcern: { w: 'majority' } }); print(db.getSiblingDB('$DB').items.countDocuments())"

echo "2. running the backup now"
kubectl -n "$NS" create job "${NAME}-backup-test-${STAMP}" --from="cronjob/${NAME}-backup" >/dev/null
kubectl -n "$NS" wait --for=condition=complete --timeout=180s "job/${NAME}-backup-test-${STAMP}"
kubectl -n "$NS" logs "job/${NAME}-backup-test-${STAMP}" | tail -3

echo "3. dropping $DB"
mongo "db.getSiblingDB('$DB').dropDatabase(); print('count after drop: ' + db.getSiblingDB('$DB').items.countDocuments())"

echo "4. restoring the newest archive"
cat <<YAML | kubectl -n "$NS" apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata:
  name: ${NAME}-restore-test-${STAMP}
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: mongorestore
          image: mongo:$(kubectl -n "$NS" get mrs "$NAME" -o jsonpath='{.spec.version}')
          imagePullPolicy: IfNotPresent
          command: ["sh", "-c"]
          args:
            - |
              set -eu
              f=\$(ls -1t /backup/dump-*.archive.gz | head -1)
              echo "restoring \$f"
              mongorestore --uri '${URI}' --gzip --archive="\$f" --nsInclude '${DB}.*' --drop
          volumeMounts:
            - { name: backup, mountPath: /backup }
      volumes:
        - name: backup
          persistentVolumeClaim: { claimName: ${NAME}-backup }
YAML
kubectl -n "$NS" wait --for=condition=complete --timeout=180s "job/${NAME}-restore-test-${STAMP}"
kubectl -n "$NS" logs "job/${NAME}-restore-test-${STAMP}" | tail -2

echo "5. verifying"
got=$(mongo "print(db.getSiblingDB('$DB').items.countDocuments())" | tail -1)
kubectl -n "$NS" delete job "${NAME}-backup-test-${STAMP}" "${NAME}-restore-test-${STAMP}" >/dev/null
if [ "$got" = "$N" ]; then
  echo "PASS: restored $got of $N documents"
else
  echo "FAIL: restored $got of $N documents"
  exit 1
fi
