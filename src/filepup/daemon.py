import signal
import time


def main() -> None:
    running = True

    def stop(_signum, _frame) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    print("FilePup service running 🐾", flush=True)
    while running:
        time.sleep(1)
    print("FilePup service stopped", flush=True)
