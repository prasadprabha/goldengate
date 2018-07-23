package com.storm.cassandra;

import java.util.Map;

import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.apache.log4j.Logger;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;


public class StBolt implements IBasicBolt {
	
	private static final long serialVersionUID = 1L;

	private static final Logger logger = Logger
			.getLogger(StBolt.class);
	
	private static Session session = null;
	private Cluster cluster = null;
	String cassandraURL;
	JSONObject eventJson = null;
	String topicname = null;

	Row row = null;
	
	
	com.datastax.driver.core.ResultSet segmentlistResult = null;
	com.datastax.driver.core.ResultSet newCountUpdatedResult = null;
	

	public StBolt(String topicname) {
		this.topicname = topicname;
	}

	public void prepare(Map stormConf, TopologyContext topologyContext) {
       
		
		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		System.out.println("load cassandra ip");
		session = cluster.connect();
		System.out.println("CassandraCounterBolt prepare method ended");

	}

	public void execute(Tuple input, BasicOutputCollector collector) {
       
		
		Fields fields = input.getFields();

		try {
			eventJson = (JSONObject) JSONSerializer.toJSON((String) input
					.getValueByField(fields.get(0)));
			topicname = (String) eventJson.get("topicName");

			String ievent = "ievent";
			String install = "install";

			segmentlistResult = session
					.execute("insert into ievent.install(topicname)values('"+topicname+"')");

		} catch (Exception e) {
			e.printStackTrace();
		}
		

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public void cleanup() {

	}

}
