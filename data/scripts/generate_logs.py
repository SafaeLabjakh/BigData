import random, time

levels = ['INFO', 'WARN', 'ERROR']
messages = ['Connexion OK', 'Erreur DB', 'Utilisateur non trouve', 'Temps de reponse long']

with open('logs.csv', 'w') as f:
    f.write('timestamp,level,message\n')
    for i in range(100000):  # 10 000 lignes
        ts = time.strftime('%Y-%m-%d %H:%M:%S')
        level = random.choice(levels)
        msg = random.choice(messages)
        f.write(f'{ts},{level},{msg}\n')
