from fox_consumer import Consumer
from pika.exceptions import ConnectionClosed

def main():
    try:
        cons = Consumer()
        cons.runner()
    except KeyboardInterrupt as e:
        print "Goodby - user request..."
    except ConnectionClosed as e:
        print "No rabbitmq server"
    except Exception as e:
        cons.close_connections()
        print "\nException cause:\n", e


if __name__ == "__main__":
    main()
