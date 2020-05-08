 '''
 Create task3 as consumer2 which reads cons1 sent data Here
Task2 acts like producer and consumer both
'''
import json
import concurrent.futures
import logging
import queue
import random
import threading
import time

data = {}
data['people'] = []
data['people'].append({
    'name': 'Scott',
    'website': 'stackabuse.com',
    'from': 'Nebraska'
})
data['people'].append({
    'name': 'Larry',
    'website': 'google.com',
    'from': 'Michigan'
})
data['people'].append({
    'name': 'Tim',
    'website': 'apple.com',
    'from': 'Alabama'
})
with open('data2.json', 'w') as outfile:
    json.dump(data, outfile)

def dataParser(need_parse):
  new_data = {}
  print("parsing data")
  new_data[u'name'] = need_parse[u'name']
  new_data[u'from'] = need_parse[u'from']
  data_conv = json.dumps(new_data)
  return data_conv

def producer(queue, event):
    """Pretend we're getting a number from the network."""
    while not event.is_set():
      f = open ('data2.json', "r")   
      data = json.loads(f.read()) 
      #print(data)
      message = json.dumps(data).encode("utf8")
      my_tmp_file = tempfile.NamedTemporaryFile()
      my_tmp_file.write(message)
      my_tmp_file.seek(0)
      logging.info("Producer got message:")# %s", message)
      queue.put(my_tmp_file.read())
      my_tmp_file.close()
      
      # queue.put(filename)#queue.put(message)
    logging.info("Producer received event. Exiting")

def consumer1(queue1,queue2, event):
    """Pretend we're saving a number in the database."""
    while not event.is_set() or not queue.empty():
        message = queue1.get()
        print("\ncon1: here0")
        logging.info("Consumer1 storing message: %s (size=%d)", message, queue1.qsize())
        print("\ncon1: here1")
        with open(message, encoding='utf-8') as data_file:
          data = json.loads(data_file.read())
          data_json = json.dumps(message)
          print("\ncon1: here2")
          print(data_json)
          message2 = DataParser(data_json)
          print("\ncon1: here3")
          #message2 = random.randint(1, 101)
          logging.info("Consumer1 sending message:%s", message2)
          queue2.put(message2)
          print("\ncon1: here4")
    logging.info("Consumer1 received event. Exiting")

def consumer2(queue, event):
    """Pretend we're saving a number in the database."""
    print("\ncon2:here0")
    while not event.is_set() or not queue.empty():
        message = queue.get()
        print("\ncon2: here1")
        logging.info(
            "Consumer 2 storing message: %s (size=%d)", message, queue.qsize()
        )

    logging.info("Consumer 2 received event. Exiting")

if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    pipeline1 = queue.Queue(maxsize=10)
    pipeline2 = queue.Queue(maxsize=5)
    
    event = threading.Event()
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        executor.submit(producer, pipeline1, event)
        executor.submit(consumer1, pipeline1,pipeline2, event)
        executor.submit(consumer2, pipeline2, event)

        time.sleep(0.1)
        logging.info("Main: about to set event")
        event.set()


