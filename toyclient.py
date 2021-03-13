import click
import requests
from config import *
import json
import socket


ip = socket.gethostbyname(socket.gethostname() + ".local")


@click.group()
def main():
     "A ToyChord Client Command Line Interface"

@main.command()
@click.option("--key")
@click.option("--value")
@click.option("--port")
def insert(key, value, port):
    "This command inserts a (key,value) pair in Chord"
    requested_url = "http://{0}:{1}/node/insert_pair/{2}/{3}/{4}".format(ip, port, key, value, REPLICATION)
    reply = requests.post(requested_url)
    if reply.status_code==200:
        click.echo("Pair inserted successfully")
    else:
        click.echo("Fail to insert the pair!") 


@main.command()
@click.option("--key")
@click.option("--port")
def query(key, port):
    "This query command with a given key returns the value that is stored in Chord"
    if key == "*":
        requested_url = "http://{0}:{1}/node/query_key/{2}".format(ip, port, key)
        reply = requests.get(requested_url)
        x = json.loads(reply.text)
        for key in x:
            print(key, ":",  x[key])
            print()
    else:
        requested_url = "http://{0}:{1}/node/query_key/{2}".format(ip, port, key)
        reply = requests.get(requested_url)
        click.echo(reply.text)



if __name__ == "__main__":
    main()