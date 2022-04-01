#!/usr/bin/env python3

# This script dumps various streams
import os
import sys
import argparse
import zmq
import pickle
import numpy
import json
import datetime as dt
import time as ti
import cv2
from utilityRt import time_queue, ctx, create_folder, unrecognized_datebase, filedate
# Handle arguments

PROG_DESCRIPTION='''\
description:
  this program subscribes ZMQ streams and dumps the data to stdout
'''

PROG_EPILOG='''\

examples:
  %(prog)s
  %(prog)s --pretty-json
  %(prog)s -ic tcp://192.168.10.11:9877 tcp://192.168.10.12:9877
'''

DEFAULT_INGRESS_ADDR = 'tcp://localhost:9800'

ap = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=PROG_DESCRIPTION,
        epilog=PROG_EPILOG)

ap.add_argument('-v','--verbose', action='count', default=0, help='increase verbosity')

apg1 = ap.add_argument_group()
apmx1 = apg1.add_mutually_exclusive_group()
apmx1.add_argument('-ic', dest='ingress_connects', metavar='ADDR', nargs='+', action='append',
                   help='specify a ZMQ endpoint to connect to a data source (default:"{}")'
                       .format(DEFAULT_INGRESS_ADDR))

apg3 = ap.add_argument_group()
apg3.add_argument('--pretty-json', action='store_true',
                  help='dump the json with sorted keys and indent')

args = ap.parse_args()


# Flatten the lists
if args.ingress_connects:
    args.ingress_connects = sum(args.ingress_connects, [])
    args.ingress_connects = [s for s in args.ingress_connects if s]

# Set the default endpoint
if not args.ingress_connects:
    args.ingress_connects = [ DEFAULT_INGRESS_ADDR ]


# Set the option for json.dumps
if args.pretty_json:
    json_opts = {'sort_keys': True, 'indent': 2}
else:
    json_opts = {'sort_keys': True}
    




# --------------------------------
# Utility Function
# --------------------------------

def dprint(level_, *args_, **kwargs_):
    if level_ <= 0:
        print('Log level must be positive: {}'.format(level_))
        sys.exit(1)

    # args.verbose:
    #    0: Suppress all debug logging
    #    1: Show significant logs only
    #    2: Show important logs
    #    3: Show detailed logs
    #    4: Show trace logs
    if args.verbose >= level_:
        print(*args_, **kwargs_)



# --------------------------------
# Stream Subscriber
# --------------------------------
TOPIC_LOG_FRAME = b'LogFrame'
TOPIC_VIDEO_FRAME = b'VideoFrame'
POLL_WAIT_TIME  = 30           # milliseconds


def subscribe_streams(ctx_, stream_addrs_):
    print('Starting the stream subscriber now...\n')
    
    # Setup the ingress socket for streams
    socki = ctx_.socket(zmq.SUB)
    socki.setsockopt(zmq.RCVHWM, 100)
    socki.setsockopt(zmq.SUBSCRIBE, TOPIC_LOG_FRAME)
    socki.setsockopt(zmq.SUBSCRIBE, TOPIC_VIDEO_FRAME)

    for addr in stream_addrs_:
        print('Connecting to "{}"'.format(addr))
        socki.connect(addr)

    poller = zmq.Poller()
    poller.register(socki, zmq.POLLIN)

    print('\nReceiving the stream data ...\n')
    
    while True:
        create_folder()
        events = dict(poller.poll(POLL_WAIT_TIME))

        if events.get(socki) != zmq.POLLIN:
            continue    # Poller time out


        # Receive a message
        msg = socki.recv_multipart(flags=zmq.NOBLOCK)
        if not msg:
            continue

        # Decode the message
        try:
            if len(msg) != 6:
                continue

            topic       = msg[0]  # bytes
            stream_id   = msg[1]  # bytes
            frame_time  = pickle.loads(msg[2])

            meta    = pickle.loads(msg[3])
            img     = numpy.frombuffer(msg[4], dtype=meta['dtype'])
            img     = img.reshape(meta['shape'])

            annotation = pickle.loads(msg[5])
            image      = img

            dprint(4,'Received: topic {} stream_id {} ftime {:.3f}'
                     .format(topic, stream_id, frame_time), flush=True)

            if topic.endswith(b'/' + stream_id):
                # Separate out the stream ID from the topic
                topic = topic[:-(len(stream_id)+1)]

           #  if topic == TOPIC_LOG_FRAME:
           #      item        = (numpy.array([]), pickle.loads(msg[3]))
           # else:
           #     dprint(4,'Ignoring a message by topic: {} (len {})'
           #               .format(topic, len(msg)))
           #     continue

        except pickle.UnpicklingError as e:
            print('Corrupted pickle message: topic {}, stream {}, {}'
                  .format(topic, stream_id, e))
            continue
        except IndexError as e:
            print('Invalid length: topic {}, stream {}, length {}, {}'
                  .format(topic, stream_id, len(msg)))
            continue
        except ValueError as e:
            print('Invalid value: topic {}, stream {}, {}'
                  .format(topic, stream_id, e))
            continue
#---------------------------------------------------------detection_truck----------------------------------------------------------------------                 
        # if annotation['w'] > 50 and annotation['object_name']=='truck':
        if annotation['w'] > 50:
            print('{}'.format(json.dumps(annotation, **json_opts)), flush=True)

        ###
        ### Write Down the JSON and Image file
        ###
            date_string = annotation['counted_at']
            ddate, time = date_string.split()
            hhmmss, sep, dec = time.partition(".")
            recoverdate = hhmmss.replace(":","")

            file_path = './data/'+filedate+'/log_' + str(stream_id.decode()) + '_' + filedate + '_'+ recoverdate +'_'+annotation['direction']+'_'+annotation['object_name']+'_'+annotation['label_name'].replace(' ','')
            json_file, jpg_file, crop_file = (file_path +'.json', file_path+'.jpg',file_path+'_crop.jpg')

        # Write Log
            with open(json_file, mode='wt', encoding='utf-8') as file:
                json.dump(annotation, file, ensure_ascii=False, indent=2)

        # Write JPG
            crop_image = image[annotation['y']:annotation['y']+annotation['h'],annotation['x']:annotation['x']+annotation['w']] 
            cv2.imwrite(crop_file, crop_image)
            cv2.imwrite(jpg_file, image)



        # Log file names
            with open(f'./data/{filedate}/{filedate}.txt', 'a') as f:
                f.writelines(f"{json_file}\n")

            unrecognized_datebase(json_file)


            if time_queue.full():
                print("======================full=====================")
            else:
                time_queue.put(str(stream_id.decode()) + '_' + filedate + '_'+ recoverdate+'_'+annotation['direction']+'_'+annotation['object_name']+'_'+annotation['label_name'].replace(' ',''))
                print('detection_truck process id:',os.getpid())


#---------------------------------------------------------detection_truck----------------------------------------------------------------------     
    # Clean up
    socki.setsockopt(zmq.LINGER, 0)
    socki.close()

    dprint(4,'Exiting the stream subscriber.')


def detection():
    subscribe_streams(ctx, args.ingress_connects)
    ctx.term()

    # EOF
