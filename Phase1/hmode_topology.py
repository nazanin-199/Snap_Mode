"""
Word count topology
"""

from streamparse import Grouping, Topology

from bolts.hmode_bolt import WordCountBolt as HmodeWordCountBolt
from spouts.hmode_spout import WordSpout as  HmodeWordSpout



 
class WordCount(Topology): 

    word_spout_hmode = HmodeWordSpout.spec()
    count_bolt_hmode = HmodeWordCountBolt.spec(inputs=word_spout_hmode, par=3 )
    
