from prometheus_client import start_http_server, Counter
import random, time

# Compteur des logs
log_counter = Counter('log_level', 'Nombre de logs par niveau', ['level'])

# Expose sur port 8000
start_http_server(8000)

levels = ['INFO', 'WARN', 'ERROR']

while True:
    level = random.choice(levels)
    log_counter.labels(level=level).inc()
    time.sleep(1)
