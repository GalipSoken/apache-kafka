#!/usr/bin/env bash
# SCRAM kullanıcıları ve topic bazlı ACL'ler (en az yetki prensibi)
# Dosya: code/security/acl_ornekleri.sh
set -euo pipefail
B="--bootstrap-server broker-1.kafka.example.com:9092 --command-config client-sasl-ssl.properties"

# 1) Kullanıcılar (SCRAM kimlik bilgileri KRaft metadata'sında saklanır)
kafka-configs.sh $B --alter --add-config 'SCRAM-SHA-512=[iterations=8192,password=DEGISTIRIN]' --entity-type users --entity-name svc-odeme-producer
kafka-configs.sh $B --alter --add-config 'SCRAM-SHA-512=[iterations=8192,password=DEGISTIRIN]' --entity-type users --entity-name svc-fraud-consumer

# 2) Producer: sadece islemler.* topic'lerine yazabilir + idempotent yazım
kafka-acls.sh $B --add --allow-principal User:svc-odeme-producer \
  --operation Write --operation Describe --topic islemler. --resource-pattern-type prefixed
kafka-acls.sh $B --add --allow-principal User:svc-odeme-producer --operation IdempotentWrite --cluster

# 3) Consumer: sadece islemler.json okur, sadece kendi group.id'sini kullanır
kafka-acls.sh $B --add --allow-principal User:svc-fraud-consumer \
  --operation Read --operation Describe --topic islemler.json
kafka-acls.sh $B --add --allow-principal User:svc-fraud-consumer \
  --operation Read --group fraud-skorlama

# 4) Transactional producer ise ayrıca:
# kafka-acls.sh $B --add --allow-principal User:svc-x --operation Write --operation Describe --transactional-id svc-x- --resource-pattern-type prefixed

kafka-acls.sh $B --list --topic islemler.json
