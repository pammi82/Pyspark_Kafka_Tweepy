from pyspark import SparkConf, SparkContext,SQLContext                                                                                                                     
from pyspark.streaming import StreamingContext                                                                                                                             
from pyspark.streaming.kafka import KafkaUtils                                                                                                                             
from pyspark.sql.functions import desc                                                                                                                                     
from pyspark.sql import Row                                                                                                                                                
import operator                                                                                                                                                            
import os,sys                                                                                                                                                              
import traceback                                                                                                                                                           
import json                                                                                                                                                                
                                                                                                                                                                           
                                                                                                                                                                           
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'                                                       
conf = SparkConf().setMaster("yarn").setAppName("TwitterSentimentAnalysis").set("spark.driver.allowMultipleContexts", "true")                                          
sc = SparkContext.getOrCreate(conf=conf)                                                                                                                                   
sc.setLogLevel("WARN")                                                                                                                                                     
sqlContext = SQLContext(sc)                                                                                                                                                
ssc = StreamingContext(sc, 10)                                                                                                                                             
#ssc.checkpoint("test_final")                                                                                                                                              
                                                                                                                                                                           
                                                                                                                                                                           
                                                                                                                                                                           
                                                                                                                                                                           
                                                                                                                                                                           
                                                                                                                                                                           
def stream(ssc):                                                                                                                                                           
                                                                                                                                                                           
    try:                                                                                                                                                                   
                                                                                                                                                                           
                                                                                                                
        zkQuorum="sandbox.hortonworks.com:2181"                                                                                                                            
                                                                                                                                                                           
        topic="python"                                                                                                                                                     
        kstream = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})                                                                           
        parsed = kstream.map(lambda v: json.loads(v[1]))                                                                                                                                                               
        parsedLocationNone = parsed.filter(lambda x : x['user']['location'] is not None)
        parsedCustomLocation = parsedLocationNone.filter(lambda x: "newyork" in  x['user']['location'].strip().lower().replace(" ","") or "ny" in x['user']['location'].strip().lower().replace(" ","") )                  
        parsedCustomLocation.map(lambda x: (x['text'].encode('utf-8',"ignore"), x['user']['location'].encode('utf-8',"ignore")) ).pprint(3)                                      
                                                                                                                                                                           
                                                                                                                                                                           
                                                                                                                                                                           
                                                                                                                                                                           
        # Start the computation                                                                                                                                            
       	ssc.start()                                                                                                                                                        
        ssc.awaitTermination()                                                                                                                                             
        ssc.stop(stopGraceFully = True)                                                                                                                                    
                                                                                                                                                                           
        return True     

    except:                                                                                                                                                                
        print('------------Exception in stream block---------------')                                                                                                      
        exc_type, exc_value, exc_traceback = sys.exc_info()                                                                                                                
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)                                                                                             
        print ('\n'.join('!! ' + line for line in lines))                                                                                                                  
        print('--------Stopping Contexts--------')                                                                                                                         
        ssc.stop()                                                                                                                                                         
        sc.stop()                                                                                                                                                          
                                                                                                                                                                           
if __name__=="__main__":                                                                                                                                                   
                                                                                                                                                                           
    try:                                                                                                                                                                   
        stream(ssc)                                                                                                                                                        
    except:                                                                                                                                                                
        print('------------Exception in main block-----------')                                                                                                            
        exc_type, exc_value, exc_traceback = sys.exc_info()                                                                                                                
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)                                                                                             
        print ('\n'.join('!! ' + line for line in lines))                                                                                                                  
        print("-------Stopping contexts--------")                                                                                                                          
        ssc.stop()                                                                                                                                                         
        sc.stop()                                                                                                                                                          
                      