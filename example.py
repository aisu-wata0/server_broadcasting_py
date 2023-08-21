if __name__ == "__main__":
    import logging
    import json
    import datetime
    import time

    from . import Server

    logger = logging.getLogger("server_broadcasting_py-test")

    HOST = "localhost"
    PORT = 7866
    socket_address = (HOST, PORT)

    DEBUG = True
    sleep_time = 1
    """
	Don't process continuously, sleep a bit
	Infinite loops are bad for processors
	"""

    server = Server(socket_address)
    server.start()

    while True:
        try:
            if len(server.clients) > 0 or DEBUG:
                timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
                data_to_broadcast = {
                    "timestamp": timestamp,
                }
                logger.info(f"{json.dumps(data_to_broadcast, indent=2,)}")
                server.broadcast(data_to_broadcast)
            # Infinite loops are bad for processors
            time.sleep(sleep_time)
        except Exception as e:
            logger.exception(e)
