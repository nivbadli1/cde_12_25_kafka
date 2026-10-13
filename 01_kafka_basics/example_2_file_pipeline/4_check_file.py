"""
Example 2 — Verify the result
==============================
RUN ORDER: step 4 of 4  ->  run any time after the pipeline has been running for ~30 seconds
(the other 3 scripts can keep running).

What it does:
    Prints the target file written by 2_consumer.py.
    If the pipeline works, you will see the same "Event #N|hostN|..." lines
    that 1_log_generator.py wrote to the source file — they travelled through Kafka.
"""
target_file = '/home/developer/kafka/sinkFiles/tarFile.log'

with open(target_file) as f:
    content = f.read()

print(content if content else '(file is empty — are 1_log_generator, 2_consumer and 3_producer all running?)')
