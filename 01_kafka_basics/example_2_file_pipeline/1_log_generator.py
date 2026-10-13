"""
Example 2 — Log Generator (the data SOURCE)
============================================
Pipeline:  1_log_generator  ->  srcFile.log  ->  3_producer  ->  Kafka  ->  2_consumer  ->  tarFile.log

RUN ORDER: step 1 of 4  ->  Terminal 1. Leave it running.
    It must run before 3_producer.py, because the producer opens srcFile.log and the file
    has to exist first.

What it does:
    Simulates an external system (e.g. a web server) that keeps writing log lines to a file,
    one line every 0-5 seconds, forever. Stop it with Ctrl+C.

Before the first run, create the folders once:
    mkdir -p /home/developer/kafka/srcFiles /home/developer/kafka/sinkFiles
"""
from random import randint, random
from time import sleep
from datetime import datetime

log_path = '/home/developer/kafka/srcFiles/srcFile.log'


def event_generator():
    """
    Infinite generator: every call to next() yields a tuple (counter, hostname, ts).
    'yield' pauses the function and resumes it on the next iteration,
    so we can produce endless data without keeping it all in memory.
    """
    counter = 1
    while True:
        hostname = f'host{randint(1, 5)}'                          # host1 ... host5
        ts = datetime.now().strftime('%a %b %d %H:%M:%S %Y')        # e.g. Tue Oct 13 16:20:01 2026
        yield counter, hostname, ts
        counter += 1


# 'w' = overwrite the file on every run (start from a clean log)
with open(log_path, 'w') as f:
    for counter, hostname, ts in event_generator():
        line = f'Event #{counter}|{hostname}|{ts}\n'
        f.write(line)
        print(line, end='')

        # flush() pushes the line from Python's memory buffer to the file on disk NOW,
        # so the producer (another process) can see it immediately.
        f.flush()

        sleep(5 * random())      # random pause between 0 and 5 seconds
