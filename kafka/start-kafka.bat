@echo off
REM =========================================
REM Script pour démarrer Kafka en mode KRaft
REM =========================================

REM Chemin vers l'installation Kafka globale
SET KAFKA_HOME=C:\kafka\

REM Définir un UUID (à utiliser une seule fois pour formater)
SET UUID=ABTZo-jyQw-dzPOSu8fP-Q

REM ==========================
REM Étape 1 : Formater le stockage Kafka (décommenter une seule fois)
REM ==========================
"%KAFKA_HOME%bin\windows\kafka-storage.bat" format --ignore-formatted -t %UUID% -c "C:\kafka\config\server.properties"

REM ==========================
REM Étape 2 : Lancer Kafka
REM ==========================
"%KAFKA_HOME%bin\windows\kafka-server-start.bat" "C:\Users\HP\Desktop\BigDataLogsProject\kafka\config\server.properties"

pause
