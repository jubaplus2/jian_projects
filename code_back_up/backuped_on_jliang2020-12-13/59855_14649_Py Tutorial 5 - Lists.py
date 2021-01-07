#Coach for team keeping track of all players
players = [11, 45, 69, 81]
players #Players is now a new defined variable

#Can get exact position, just like strings
players[2]
players[1 : 4]

#Can change old value to new value
players[1] = 30
players #Position 1 is now 30 instead of 45
players[: 2] = [0, 0]

#Change list permanently, .append adds something on, only one argument
players.append(100)
players

#Delete entire list
players = []