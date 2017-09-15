
import time
import pika
path = 'F:\office2\mcxSender\MCX_20170829 - Copy.rt'
infile = open(path,'r')

#queue
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='hello')

while True:
    for line in infile.readlines():
        try:
            split = line.split(',')
            if len(split)>1:#if condition is used so that the we dont split the blank line.
                client = split[19].strip()                
                #if 'M04223' == client:        
                tradeno = split[0].strip()
                comdty = split[4].strip()
                #date1 = split[5].strip()
                side = 'SELL'
                if split[15].strip() == '1':
                    side = 'BUY'                       
                qty = split[16].strip() 
                price = split[17].strip()       
                expiry = split[5].strip()
                tradetime = split[24].strip()
                msg = tradetime + "-"+ tradeno + "-"+client + "-" + comdty +'-' +expiry+"-" + side + '-' + qty  + '-' +price   
                localtime = time.localtime(time.time())  
                print("msg routed :" + msg)
                channel.basic_publish(exchange='',
                              routing_key='hello',
                              body=msg)
        except IndexError:
            pass            
    time.sleep(0.50)
    
print('end of while reached')
connection.close()  #closing rabbitmq connection       
print('rabbitmq connection closed')

    
