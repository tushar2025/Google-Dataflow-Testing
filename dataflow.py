from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.io.gcp.pubsub import ReadStringsFromPubSub
import datetime
import time
import re

class extracttimestamp(beam.DoFn):
    def process(self, element):
        dtarr = [re.search('(?<=date=)(.*)(?= time=)', element).group(0),
                 re.search('(?<=time=)(.*)(?= timezone=)', element).group(0)]
        dt = time.mktime((datetime.datetime.strptime(dtarr[0]+' '+dtarr[1], '%Y-%m-%d %H:%M:%S')).timetuple())
        yield beam.window.TimestampedValue(element,dt)
           
class ParsingFn(beam.DoFn):
    def process(self,element):
        li = [re.search('(?<=date=)(.*)(?= time=)', element).group(0),
          re.search('(?<=time=)(.*)(?= timezone=)', element).group(0),
          re.search('(?<=timezone=)(.*)(?= device_name=)', element).group(0),
          re.search('(?<=device_name=)(.*)(?= device_id=)', element).group(0),
          re.search('(?<=device_id=)(.*)(?= log_id=)', element).group(0),
          re.search('(?<=log_id=)(.*)(?= log_type=)', element).group(0),
          re.search('(?<=log_type=)(.*)(?= log_component=)', element).group(0),
          re.search('(?<=log_component=)(.*)(?= log_subtype=)', element).group(0),
          re.search('(?<=log_subtype=)(.*)(?= status=)', element).group(0),
          re.search('(status=)(.+)',element).group(0)]
        
        if(li[6]=='"Firewall"'):
            try:
                li1 = li[:9]
                li1.append(re.search('(?<=status=)(.*)(?= priority=)', li[9]).group(0))
                li1.append(re.search('(?<=priority=)(.*)(?= duration=)', li[9]).group(0))
                li1.append(re.search('(?<=duration=)(.*)(?= fw_rule_id=)', li[9]).group(0))
                li1.append(re.search('(?<=fw_rule_id=)(.*)(?= user_name=)', li[9]).group(0))
                li1.append(re.search('(?<=user_name=)(.*)(?= user_gp=)', li[9]).group(0))
                li1.append(re.search('(?<=user_gp=)(.*)(?= iap=)', li[9]).group(0))
                li1.append(re.search('(?<=iap=)(.*)(?= ips_policy_id=)', li[9]).group(0))
                li1.append(re.search('(?<=ips_policy_id=)(.*)(?= appfilter_policy_id=)', li[9]).group(0))
                li1.append(re.search('(?<=appfilter_policy_id=)(.*)(?= application=)', li[9]).group(0))
                li1.append(re.search('(?<=application=)(.*)(?= application_risk=)', li[9]).group(0))
                li1.append(re.search('(?<=application_risk=)(.*)(?= application_technology=)', li[9]).group(0))
                li1.append(re.search('(?<=application_technology=)(.*)(?= application_category=)', li[9]).group(0))
                li1.append(re.search('(?<=application_category=)(.*)(?= in_interface=)', li[9]).group(0))
                li1.append(re.search('(?<=in_interface=)(.*)(?= out_interface=)', li[9]).group(0))
                li1.append(re.search('(?<=out_interface=)(.*)(?= src_mac=)', li[9]).group(0))
                li1.append(re.search('(?<=src_mac=)(.*)(?= src_ip=)', li[9]).group(0))
                li1.append(re.search('(?<=src_ip=)(.*)(?= src_country_code=)', li[9]).group(0))
                li1.append(re.search('(?<=src_country_code=)(.*)(?= dst_ip=)', li[9]).group(0))
                li1.append(re.search('(?<=dst_ip=)(.*)(?= dst_country_code=)', li[9]).group(0))
                li1.append(re.search('(?<=dst_country_code=)(.*)(?= protocol=)', li[9]).group(0))
                li1.append(re.search('(?<=protocol=)(.*)(?= src_port=)', li[9]).group(0))
                li1.append(re.search('(?<=src_port=)(.*)(?= dst_port=)', li[9]).group(0))
                li1.append(re.search('(?<=dst_port=)(.*)(?= sent_pkts=)', li[9]).group(0))
                li1.append(re.search('(?<=sent_pkts=)(.*)(?= recv_pkts=)', li[9]).group(0))
                li1.append(re.search('(?<=recv_pkts=)(.*)(?= sent_bytes=)', li[9]).group(0))
                li1.append(re.search('(?<=sent_bytes=)(.*)(?= recv_bytes=)', li[9]).group(0))
                li1.append(re.search('(?<=recv_bytes=)(.*)(?= tran_src_ip=)', li[9]).group(0))
                li1.append(re.search('(?<=tran_src_ip=)(.*)(?= tran_src_port=)', li[9]).group(0))
                li1.append(re.search('(?<=tran_src_port=)(.*)(?= tran_dst_ip=)', li[9]).group(0))
                li1.append(re.search('(?<=tran_dst_ip=)(.*)(?= tran_dst_port=)', li[9]).group(0))
                li1.append(re.search('(?<=tran_dst_port=)(.*)(?= srczonetype=)', li[9]).group(0))
                li1.append(re.search('(?<=srczonetype=)(.*)(?= srczone=)', li[9]).group(0))
                li1.append(re.search('(?<=srczone=)(.*)(?= dstzonetype=)', li[9]).group(0))
                li1.append(re.search('(?<=dstzonetype=)(.*)(?= dstzone=)', li[9]).group(0))
                li1.append(re.search('(?<=dstzone=)(.*)(?= dir_disp=)', li[9]).group(0))
                yield str([{
                            'eventdate': li1[0],'eventtime': li1[1],'timezone': li1[2],'device_name': li1[3],'device_id': li1[4],
                            'log_id': li1[5],'log_type': li1[6],'log_component': li1[7],'log_subtype': li1[8],'status': li1[9],
                            'priority': li1[10],'duration': li1[11],'fw_rule_id': li1[12],'user_name': li1[13],'user_gp': li1[14],
                            'iap': li1[15],'ips_policy_id': li1[16],'appfilter_policy_id': li1[17],'application': li1[18],
                            'application_risk': li1[19],'application_technology': li1[20],'application_category': li1[21],
                            'in_interface': li1[22],'out_interface': li1[23],'src_mac': li1[24],'src_ip': li1[25],
                            'src_country_code': li1[26],'dst_ip': li1[27],'dst_country_code': li1[28],'protocol': li1[29],
                            'src_port': li1[30],'dst_port': li1[31],'sent_pkts': li1[32],'recv_pkts': li1[33],'sent_bytes': li1[34],
                            'recv_bytes': li1[35],'tran_src_ip': li1[36],'tran_src_port': li1[37],'tran_dst_ip': li1[38],
                            'tran_dst_port': li1[39],'srczonetype': li1[40],'srczone': li1[41],'dstzonetype': li1[42],
                            'dstzone': li1[43]}])
            except:
                yield str(element)
        
def run(argv=None):
    """Main entry point; defines and runs the wordcount pipeline."""
    print "enter"
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='gs://practice-00001/C2ImportCalEventSample.csv',
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    topic = known_args.input[len('pubsub://'):]
    pipeline_args.append('--streaming')
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    
    logger1 = logging.getLogger('testlogger')
    
    p = beam.Pipeline(options=pipeline_options)
    lines = p | 'read_from_pubsub' >> ReadStringsFromPubSub(topic=topic)
    timest = lines | 'gettimestamp' >> beam.ParDo(extracttimestamp())
    win = timest | 'createwindow' >> beam.WindowInto(beam.window.FixedWindows(60),trigger=beam.trigger.AfterProcessingTime(10),accumulation_mode=beam.trigger.AccumulationMode.DISCARDING)
    par = win | 'parsing' >> beam.ParDo(ParsingFn())
    #par | 'writetobq' >> beam.io.WriteToBigQuery(table='firewall_data',dataset='cybersecurity',project='practice-00001')
    par | 'write_to_file' >> WriteToText(known_args.output)
    
    result = p.run()
    print("waiting for pipeline to complete...")
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()