import socket

host = "consolidated-northeuropec2-prod-metastore-1.mysql.database.azure.com"
port = 3306

try:
    with socket.create_connection((host, port)) as sock:
        print("Connected to:",host,":",port)
except:
    print("Connection to",host,":",port,"failed")
    
#%sh
#nc -vz "consolidated-northeuropec2-prod-metastore-1.mysql.database.azure.com" 3306


#%sh
#nslookup consolidated-northeuropec2-prod-metastore-1.mysql.database.azure.com

# %sh
# telnet 52.138.224.6 3306

# %sh
# netstat -a

# %sh
# traceroute consolidated-northeuropec2-prod-metastore-1.mysql.database.azure.com
