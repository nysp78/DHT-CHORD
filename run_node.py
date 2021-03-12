from argparse import ArgumentParser
import NodeApi

if __name__ == '__main__':
  parser = ArgumentParser()
  
  parser.add_argument('-ip', '--ip_address', action='store', type=str, required=True)
  parser.add_argument('-p', '--port', action='store', type=int, required=True)
  parser.add_argument('-b', '--bootstrap', action='store', type=int, required=True)
  #parser.add_argument('-c', '--consistency', action='store', type=str, requires=True)
  #parser.add_argument('-k', '--replication', action='store', type=int, requires=True)
  

  args = parser.parse_args()
  NodeApi.app.config["SERVER_NAME"] = "{0}:{1}".format(args.ip_address, args.port)
  NodeApi.app.config["IP"] = args.ip_address
  NodeApi.app.config["PORT"] = args.port
  NodeApi.app.config["ISBOOT"] = args.bootstrap
  #NodeApi.app.config["CONSISTENCY"] = args.consistency
  #NodeApi.app.config["REPLICATION"] = args.replication
  NodeApi.app.run(host=args.ip_address, port=args.port)