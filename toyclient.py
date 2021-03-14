import click
import requests
from config import *
import json
import socket


#ip = socket.gethostbyname(socket.gethostname() +".local")
ip = "127.0.0.1"


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
def join():
    "Nodes inserted to Chord"
    requested_url = "http://{0}:5000/node/join/".format(ip)
    reply = requests.post(requested_url)

    if reply.status_code == 200:
        click.echo("Nodes inserted successfully")
    else:
        click.echo("Fail to insert the Nodes!") 


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



@main.command()
@click.option("--key")
@click.option("--port")
def delete(key, port):
    "delete key from Chord"
    requested_url = "http://{0}:{1}/node/delete/{2}".format(ip, port, key)
    reply = requests.delete(requested_url)

    if reply.status_code==200:
        click.echo("Key deleted successfully from Chord")
    else:
        click.echo("Fail to fail to delete key!")


@main.command()
@click.option("--port")
def depart(port):
    "Node depart gracefully from Chord"
    requested_url = "http://{0}:{1}/node/depart/".format(ip, port)
    reply = requests.post(requested_url)

    if reply.status_code==200:
        click.echo(click.style("Node departed gracefully from Chord", fg='green'))

    else:
        click.echo("Failure in node departure!")


@main.command()
@click.option("--port")
def overlay(port):
    "Prints the network topology"
    requested_url = "http://{0}:{1}/node/net_overlay".format(ip, port)
    reply = requests.get(requested_url)

    if reply.status_code==200:
        print("Network Topology")
        ls = reply.text.split(", ")
        ls[0] = ls[0].replace("[",'')
        ls[len(ls)-1] = ls[len(ls)-1].replace("]",'')
        for item in ls:
            print(item)
           
    else:
        print("Error")



if __name__ == "__main__":
    main()

click.echo(click.style('Hello World!', fg='green'))
click.echo(click.style('Some more text', bg='blue', fg='white'))
click.echo(click.style('ATTENTION', blink=True, bold=True))
