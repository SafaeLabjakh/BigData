#!/bin/bash
# Script pour démarrer Zookeeper

# Définir le dossier Kafka (modifie si nécessaire)
KAFKA_DIR=$(pwd)

echo "Démarrage de Zookeeper..."
$KAFKA_DIR/bin/zookeeper-server-start.sh $KAFKA_DIR/config/zookeeper.properties
