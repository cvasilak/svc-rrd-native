// Service RRD

#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <string.h>

#include <uuid.h>
#include <rrd.h>
#include <stomp.h>
#include <jansson.h>

#include "svc-rrd-native.h"

const NE *get_ne_for_prefix_id(const NES *list, char *prefix_id) {
	NE *ne;
	
	for (int i = 0; i < list->capacity; i++) {
		ne = list->ne[i];
		
		if (strcmp(ne->prefix_id, prefix_id) == 0) {
			return ne;
		}
	}
	
	return NULL;
}

const GROUP *get_group_for_group_name(const NE *ne, const char *name) {
	GROUP *group;
	
	for (int i = 0; i < ne->capacity; i++) {
		group = ne->groups[i];
		
		if (strcmp(group->name, name) == 0) {
			return group;
		}
	}
}

const GROUP *get_group_for_rra_name(const NE *ne, const char *name) {
	GROUP *group;
	
	for (int i = 0; i < ne->capacity; i++) {
		group = ne->groups[i];
		
		for (int j = 0; j < group->capacity; j++) {
			RRA *rra = group->rras[j];
			
			if (strcmp(rra->name, name) == 0) {
				return group;
			}
		}
	}

	return NULL;
}


void dump(INSTANCE *instance) {

	fprintf(stdout, "instance_id: %s\n", instance->instance_id);
	fprintf(stdout, "listening_queue: %s\n", instance->listening_queue);
	
	NES *list = instance->list;
	
	for (int i = 0; i < list->capacity; i++) {
		NE *ne = list->ne[i];
		
		fprintf(stdout, "ne_prefix: %s\n" , ne->prefix_id);

		for (int j = 0; j < ne->capacity; j++) {
			GROUP *group = ne->groups[j];
			
			fprintf(stdout, "\tgroup name: %s\n", group->name);
			fprintf(stdout, "\tgroup dir: %s\n",  group->dir);
			
			for (int k = 0; k < group->capacity; k++) {
				RRA *rra = group->rras[k];
				
				fprintf(stdout, "\t\trra_name: %s\n", rra->name);
				fprintf(stdout, "\t\trra_type: %s\n", rra->type);										
				fprintf(stdout, "\t\trra_descr: %s\n", rra->descr);
			}
		}
	}
}

int die(int exitCode, const char *message, apr_status_t reason) {
	char msgbuf[80];
	apr_strerror(reason, msgbuf, sizeof(msgbuf));
	fprintf(stderr, "%s: %s (%d)\n", message, msgbuf, reason);
	exit(exitCode);
	return reason;
}

static void terminate()
{
	apr_terminate();
}

char *read_file(char *filename) {
	char *source;
	
	FILE *fp = fopen(filename, "r");
	
	if (fp != NULL) {
		/* Go to the end of the file. */
		if (fseek(fp, 0L, SEEK_END) == 0) {
			/* Get the size of the file. */
			long bufsize = ftell(fp);
			if (bufsize == -1) { /* Error */ }
			
			/* Allocate our buffer to that size. */
			source = (char *)calloc(bufsize + 1, sizeof(char));
			
			/* Go back to the start of the file. */
			if (fseek(fp, 0L, SEEK_SET) == 0) { /* Error */ }
			
			/* Read the entire file into memory. */
			size_t newLen = fread(source, sizeof(char), bufsize, fp);
			if (newLen == 0) {
				fprintf(stdout, "error reading file!");
			} else {
				source[++newLen] = '\0'; /* Just to be safe. */
			}
		}
		fclose(fp);
	} else {
		fprintf(stdout, "error opening file \"%s\"\n", filename);
		exit(1);
	}
	
	return source;
	
	//free(source); /* Don't forget to call free() later! */
}

char *read_file_bin(char *filename, unsigned long *fileLen) {
	FILE *file;
	char *buffer;
	
	//Open file
	file = fopen(filename, "rb");
	
	if (!file) {
		fprintf(stderr, "Unable to open file %s", filename);
		return NULL;
	}
	
	//Get file length
	fseek(file, 0, SEEK_END);
	*fileLen=ftell(file);
	fseek(file, 0, SEEK_SET);
	
	//Allocate memory
	buffer=(char *)malloc(*fileLen+1);
	if (!buffer) {
		fprintf(stderr, "Memory error!");
		fclose(file);
		return NULL;
	}
	
	//Read file contents into buffer
	fread(buffer, *fileLen, 1, file);
	fclose(file);
	
	return buffer;	
}

int in_array(const char *value, const char **arr) {
	if (value == NULL || arr == NULL)
		return 0;
	
	do {
		if (strcmp(value, *arr++) == 0)
			return 1;

	} while (*arr != NULL);

	return 0;
}

int group_in_groups(const GROUP *group, const GROUP **groups) {
	if (group == NULL || groups == NULL)
		return 0;
		
	do {
		if (strcmp(group->name, (*groups++)->name) == 0)
			return 1;
			
	} while (*groups != NULL);
	
	return 0;
}

int rra_in_group(const char *rra, const RRA **rras) {
	if (rra == NULL || rras == NULL)
		return 0;
		
	do {
		if (strcmp(rra, (*rras++)->name) == 0)
			return 1;
			
	} while (*rras != NULL);
	
	return 0;
}

void usage() {
	fprintf(stdout, "usage: [-h broker] [-c config_dir] \n");
}

int main(int argc, char **argv) {
	if (argc ==1) {
		fprintf(stdout, "missing arguments!\n");
		usage();
		exit(1);
	}

	char *broker = NULL;
	char *host = NULL;
	
	for (int i = 1; i < argc; i++) {
		/* Check for a switch (leading "-"). */
		if (argv[i][0] == '-') {
			/* Use the next character to decide what to do. */
		    switch (argv[i][1]) {
				case 'h':
					broker = argv[++i];
					break;
				case 'c':
					host = argv[++i];
					break;
			}
		}
	}
	
	if (host == NULL || broker == NULL) {
		usage();
		exit(1);
	}

	json_error_t error;
	
	// --------------------------------------------------------------------------------------
	// read configuration file and initialize structures
	// (also used as the registration message upon
	// initial connect to the broker.
    json_t *conf_json;
   
   	char *filename;
	asprintf(&filename, "%s/registration-msg.json", host);
	
    conf_json = json_load_file(filename, 0, &error);
    if(!conf_json) {
       fprintf(stderr, "error on config: on line %d: %s\n", error.line, error.text);
        exit(1);
    }
    
    // generate UUID to uniquelly identify this instance
	char UUID[36];
	uuid_t uuidGenerated;
	uuid_generate_random(uuidGenerated);
	uuid_unparse(uuidGenerated, UUID);
	
    // this instance initialize structures
	INSTANCE *instance = (INSTANCE *)calloc(1, sizeof(INSTANCE));
    
	instance->instance_id = UUID;
	// also update registration message (so that it can be send to the server)
	json_object_set(conf_json, "instanceId",  json_string(UUID));

	instance->listening_queue = json_string_value(json_object_get(conf_json, "listeningQueue"));
		
	json_t *jnes = json_object_get(conf_json, "nes");
    
	if(json_is_array(jnes)) {
    	long list_size = json_array_size(jnes);
   		NES *list;

    	if (list_size > 0) {
			list = (NES *)calloc(1, sizeof(NES));
    		
    		list->capacity = list_size;
			list->ne = (NE **)calloc(list_size, sizeof(NE));

			for (int i = 0; i < list_size; i++) {
				json_t *jne = json_array_get(jnes, i);
				
				NE *ne = (NE *)calloc(1, sizeof(NE));
				
				ne->prefix_id = json_string_value(json_object_get(jne, "prefixId"));

				json_t *jgroups = json_object_get(jne, "groups");
				if(json_is_array(jgroups)) {
					long groups_size = json_array_size(jgroups);
	
					if (groups_size > 0) {
						ne->capacity = groups_size;
						ne->groups = (GROUP **)calloc(groups_size, sizeof(GROUP));
						
						for (int j = 0; j < groups_size; j++) {
							json_t *jgroup = json_array_get(jgroups, j);

							GROUP *group = (GROUP *)calloc(1, sizeof(GROUP));
						
							group->name = json_string_value(json_object_get(jgroup, "name"));
							const char *dir =json_string_value(json_object_get(jgroup, "dir"));
							//not interested for the hostname part (used only by the moninoring tool)
							group->dir = strdup(strchr(dir,':')+1);
							
							json_t *jrras = json_object_get(jgroup, "rras");
							
							if(json_is_array(jrras)) {
								long rras_size = json_array_size(jrras);
								
								if (rras_size > 0) {
									group->capacity = rras_size;
									group->rras = (RRA **)calloc(rras_size, sizeof(RRA));

									for (int k = 0; k < rras_size; k++) {
										json_t *jrra = json_array_get(jrras, k);
			
										RRA *rra = (RRA *)calloc(1, sizeof(RRA));
										
										rra->name = json_string_value(json_object_get(jrra, "name"));
										rra->type = json_string_value(json_object_get(jrra, "type"));
										rra->descr = json_string_value(json_object_get(jrra, "descr"));
										
										group->rras[k] = rra;
									}
								}
							}
							
							ne->groups[j] = group;
						}
					}
			
					list->ne[i] = ne;
				}
			}
			
			instance->list = list;
		}
	}
	
	dump(instance);

    // --------------------------------------------------------------------------------------
	// Setup APR 
	apr_status_t rc;
	
	apr_pool_t *pool;
	setbuf(stdout, NULL);
	
	rc = apr_initialize();
	rc==APR_SUCCESS || die(-2, "Could not initialize", rc);
	atexit(terminate);
	
	rc = apr_pool_create(&pool, NULL);
	rc==APR_SUCCESS || die(-2, "Could not allocate pool", rc);
	
	// --------------------------------------------------------------------------------------
	// Connect to the broker
	stomp_connection *connection;
	fprintf(stdout, "Connecting to \"%s\" ......", broker);
	rc=stomp_connect( &connection, broker, 61613, pool);
	rc==APR_SUCCESS || die(-2, "Could not connect", rc);
	fprintf(stdout, "OK\n");
	
	// --------------------------------------------------------------------------------------
	// Connect Message
	fprintf(stdout, "Sending Connect Message.");
	{	
		stomp_frame frame;
		frame.command = "CONNECT";
		frame.headers = apr_hash_make(pool);
		apr_hash_set(frame.headers, "login", APR_HASH_KEY_STRING, "guest");
		apr_hash_set(frame.headers, "passcode", APR_HASH_KEY_STRING, "guest");
		frame.body = NULL;
		frame.body_length = -1;
		rc = stomp_write(connection, &frame, pool);
		rc==APR_SUCCESS || die(-2, "Could not send frame", rc);
	}
	fprintf(stdout, "OK\n");
	fprintf(stdout, "Reading Response.");
	{
		stomp_frame *frame;
		rc = stomp_read(connection, &frame, pool);
		rc==APR_SUCCESS || die(-2, "Could not read frame", rc);
		fprintf(stdout, "Response: %s, %s\n", frame->command, frame->body);
	}


	// --------------------------------------------------------------------------------------
	// Registration Message
	fprintf(stdout, "Sending Registration Message.");
	{
		stomp_frame frame;
		frame.command = "SEND";
		frame.headers = apr_hash_make(pool);
		apr_hash_set(frame.headers, "ServiceRRD_msg_type", APR_HASH_KEY_STRING, "register");
		apr_hash_set(frame.headers, "destination", APR_HASH_KEY_STRING, "jms.topic.svc_rrd_ctrl_bus");
		
		frame.body_length = -1;
		frame.body = json_dumps(conf_json, JSON_INDENT(1));
		
		rc = stomp_write(connection, &frame, pool);
		rc==APR_SUCCESS || die(-2, "Could not send frame", rc);
		
		free(filename);
	}
	fprintf(stdout, "OK\n");

	// --------------------------------------------------------------------------------------
	// Subscribe to the "control bus" topic
	fprintf(stdout, "Sending Subscribe \"jms.topic.svc_rrd_ctrl_bus\" Message.");
	{
		stomp_frame frame;
		frame.command = "SUBSCRIBE";
		frame.headers = apr_hash_make(pool);
		apr_hash_set(frame.headers, "destination", APR_HASH_KEY_STRING, "jms.topic.svc_rrd_ctrl_bus");
		frame.body_length = -1;
		frame.body = "";
		rc = stomp_write(connection, &frame, pool);
		rc==APR_SUCCESS || die(-2, "Could not send frame", rc);
	}
	fprintf(stdout, "OK\n");

	// --------------------------------------------------------------------------------------
	// Subscribe to "my queue"
	char *queue = NULL;
	asprintf(&queue, "jms.queue.%s/svc_rrd", host);
	
	fprintf(stdout, "Sending Subscribe \"%s\" Message.", queue);
	{
		stomp_frame frame;
		frame.command = "SUBSCRIBE";
		frame.headers = apr_hash_make(pool);
		apr_hash_set(frame.headers, "destination", APR_HASH_KEY_STRING, queue);
		frame.body_length = -1;
		frame.body = "";
		rc = stomp_write(connection, &frame, pool);
		rc==APR_SUCCESS || die(-2, "Could not send frame", rc);
	}
	fprintf(stdout, "OK\n");

	// --------------------------------------------------------------------------------------
	// Main Loop
    json_t *root;
 	while (1) {
		fprintf(stdout, "Waiting for requests...\n");
		
		stomp_frame *reqframe;
		rc = stomp_read(connection, &reqframe, pool);
		
		//TODO: investigate error Could not read frame: Internal error (20014)
		rc==APR_SUCCESS || die(-2, "Could not send frame", rc);		
		
		// store the headers, they will be added on
		// the reply message
		char *correlation_id = (char *)apr_hash_get(reqframe->headers, "ServiceRRD_correlation_id", APR_HASH_KEY_STRING);
		char *completion_condition = (char *)apr_hash_get(reqframe->headers, "ServiceRRD_completion_condition", APR_HASH_KEY_STRING);
		char *recipients = (char *)apr_hash_get(reqframe->headers, "ServiceRRD_recipients", APR_HASH_KEY_STRING);

		//fprintf(stdout, reqframe->body);
		
		// extract JSON from the request
	    root = json_loads(reqframe->body, 0, &error);
	    
	    if(!root) {
			//fprintf(stderr, "error on received message line %d: %s\n", error.line, error.text);
			continue;
		}

		json_t *jname;

		jname = json_object_get(root, "name");

		const char *name = json_string_value(jname);
		
		if (strcmp(name, "register") == 0) {  // REGISTER Message
			// TODO: currently we ignore it use JMS Selector on instance-id
			continue;

		} else if (strcmp(name, "ping") == 0) {   // PING Message
			// PONG back server
			// TODO: send instance-id so that servers InstanceAggregator be notified
			fprintf(stdout, "PING-PONG\n");
			{
				stomp_frame frame;
				frame.command = "SEND";
				frame.headers = apr_hash_make(pool);
				apr_hash_set(frame.headers, "ServiceRRD_msg_type", APR_HASH_KEY_STRING, "PONG");
				apr_hash_set(frame.headers, "destination", APR_HASH_KEY_STRING, "jms.queue.DLQ");

				frame.body_length = -1;
				frame.body = "{ \"name\" : \"PONG\"}";
				
				rc = stomp_write(connection, &frame, pool);
				rc==APR_SUCCESS || die(-2, "Could not send frame", rc);
			}

			continue;

		}  else if (strcmp(name, "fetchLast") == 0) {

			json_t *jneId, *jconsolFunc, *jstart, *jend, *jgroups, *jrras;
			
			jneId = json_object_get(root, "neId");
			jconsolFunc = json_object_get(root, "consolFunc");
			jstart = json_object_get(root, "start");
			jend = json_object_get(root, "end");		
			jgroups = json_object_get(root, "groups");
			jrras = json_object_get(root, "rras");
	
			const char *neId = json_string_value(jneId);
			const char *consolFunc = json_string_value(jconsolFunc);
			const char **groups = NULL;
			const char **rras = NULL;		
	
			fprintf(stdout, "Received Message: %s\n%s\n", reqframe->command, reqframe->body);
			
			if(json_is_array(jgroups)) {
				groups = (const char **)calloc(json_array_size(jgroups) + 1, sizeof(char *));
	
				for(int i = 0; i < json_array_size(jgroups); i++) {
					json_t *jgroup;
					const char *group;
					
					jgroup = json_array_get(jgroups, i);
					group = json_string_value(jgroup);
					
					groups[i] = group;
				}
				
				groups[json_array_size(jgroups)] = NULL; // mark sentinel
			}
			
			if(json_is_array(jrras)) {
				rras = (const char **)calloc(json_array_size(jrras) + 1, sizeof(char *));
	
				for(int i = 0; i < json_array_size(jrras); i++) {
					json_t *jrra;
					const char *rra;
					
					jrra = json_array_get(jrras, i);
					rra = json_string_value(jrra);
					
					rras[i] = rra;
					
				}
				
				rras[json_array_size(jrras)] = NULL; // mark sentinel
			 }
			
			// Can we serve this ne_id?
			// extract prefix
			char *prefix_id = strndup(neId, 3);
			// get the NE for this prefix_id
			const NE *ne = get_ne_for_prefix_id(instance->list, prefix_id);
			
			if (ne == NULL) {  // nope we can't
				fprintf(stderr, "unknown prefix_id %s\n", prefix_id);
				continue;  // TODO: release memory till this point
			}
	
			const char **rra = NULL;
			const GROUP **rra_groups = NULL;  // NULL it otherwise realloc will play games on you!
	
			rra = rras;
			
			int len = 0;
			do {
				const GROUP *group = get_group_for_rra_name(ne, *rra++);
	
				if (group != NULL) {
					if (!group_in_groups(group, rra_groups)) {
						rra_groups = (const GROUP **)realloc(rra_groups, (len+2) * sizeof(GROUP *));
	//					fprintf(stdout, "index:%d dir:%s\n", len, group->name);
						rra_groups[len++] = group;
						rra_groups[len] = NULL; // mark sentinel
					} else { 
	//					fprintf(stdout, "already exists.\n");
					}
				} else {
	//				fprintf(stdout, "NOT found. %s\n");
				}
	
			} while (*rra != NULL);
	
			if (rra_groups == NULL) {
				fprintf(stderr, "RRAS are not served by this instance. THIS SHOULDN'T HAVE HAPPENED!\n");
				continue;
			} else {
				/*		
				const GROUP **ptr = rra_groups;
					
				do {
					fprintf(stdout, "%s\n", (*ptr)->name);
				} while (*++ptr != NULL);
				
				exit(1);
				*/
			}
			
			json_t *reply = json_object();
			json_object_set(reply, "name", json_string("fetchLastReply"));
			json_object_set(reply, "neId", json_string(neId));
			json_t *list = json_array();
	
			long start = json_integer_value(jstart);
			long end = json_integer_value(jend);
	
			// --------------------------------------------------------------------------------------
	
			const GROUP **ptr = rra_groups;
			
			// Fetch RRD
			unsigned long step, ds_cnt;
			char **ds_namv; 
			rrd_value_t *data, *datap;
	
			time_t end_time;
			time_t start_time;
	
			if (start == 0 && end == 0) {
				time_t current_time = time(NULL);
				end_time = current_time - (current_time % 300) - 300;
				start_time = end_time - 300;
			} else {
				start_time = start;
				end_time = end;
			}
			
			char *str_start_time;
			asprintf(&str_start_time, "%d", start_time);
				
			char *str_end_time;
			asprintf(&str_end_time, "%d", end_time);
			
			do {
				const GROUP *group = *ptr++;
				
				char *rrdfile = NULL;
				asprintf(&rrdfile, "%s/%s.rrd", group->dir, neId);
				
				const char *rargv[] = {"fetch", rrdfile, (char *)consolFunc, "--start", str_start_time, "--end", str_end_time};
	
				//for (int i = 0; i < rargc; i++) {
				//	printf("%s\n", rargv[i]);
				//}
			
				// Array ( [start] => 1328089800 [end] => 1328090100 [step] => 300 [ds_cnt] => 4 
				// [ds_namv] => Array ( [0] => rttmin [1] => rttavg [2] => rttmax [3] => pktloss )
				// [data] => Array ( [0] => 56 [1] => 60 [2] => 84 [3] => 0 [4] => 0 [5] => 0 [6] => 0 [7] => 0 ) 
				// [last_upd] => 01/02/12 11:55 ) 
				
				// if ( rrd_fetch(argc-1, &argv[1], &start,&end,&step,&ds_cnt,&ds_namv,&data) != -1 )
		
				if ( rrd_fetch(7, (char **) rargv, &start, &end, &step, &ds_cnt, &ds_namv, &data) != -1 ) {
					for (int i = 0; i < ds_cnt; i++) {
	
						if (in_array(ds_namv[i], rras)) { 
							json_t *rra = json_object();
							json_object_set(rra, "name",  json_string(ds_namv[i]));
	
							double value = data[i];
							
							if (isnan(value)) 
								json_object_set(rra, "value", json_string("NaN"));
							else
								json_object_set(rra, "value", json_real(value));
	
							json_array_append_new(list, rra);
						}
					}
					
				} else {
					fprintf(stdout, "error: %s\n", rrd_get_error());
					// an error has occured fetching values
					// from rrd, flag value with -1 for all
					// RRAs belonging to this group 
					const char **req_rras = rras;
					
					const RRA **group_rras = (const RRA **) group->rras;
					do {
						if (rra_in_group(*req_rras, group_rras)) {
							json_t *rra = json_object();
							json_object_set(rra, "name",  json_string(*req_rras));
							json_object_set(rra, "value", json_integer(-1));
		
							json_array_append_new(list, rra);
						}
					} while (*++req_rras != NULL);
				}
			} while (*ptr != NULL);
			
	
			json_object_set(reply, "rras", list);
			
			char *reply_body = json_dumps(reply, JSON_INDENT(1));
			
			// --------------------------------------------------------------------------------------
			// Send the reply
	
			fprintf(stdout, "Sending Reply Message:\n");
			fprintf(stdout, "%s\n", reply_body);
			
			{
				stomp_frame frame;
				frame.command = "SEND";
				frame.headers = apr_hash_make(pool);
	
				apr_hash_set(frame.headers, "ServiceRRD_msg_type", APR_HASH_KEY_STRING, "fetchLastReply");
	
				// headers used by the server Aggregator Component
				apr_hash_set(frame.headers, "ServiceRRD_correlation_id", APR_HASH_KEY_STRING, correlation_id);
				apr_hash_set(frame.headers, "ServiceRRD_completion_condition", APR_HASH_KEY_STRING, completion_condition);
				apr_hash_set(frame.headers, "ServiceRRD_recipients", APR_HASH_KEY_STRING, recipients);
				
				apr_hash_set(frame.headers, "destination", APR_HASH_KEY_STRING, "jms.queue.svc_rrd_aggr_reply");
				
				frame.body_length = -1;
				frame.body = reply_body;
				
				rc = stomp_write(connection, &frame, pool);
				rc==APR_SUCCESS || die(-2, "Could not send frame", rc);
				
			}
	
			json_decref(root);
			json_decref(reply);
			
		}  else if (strcmp(name, "fetchGraphSimple") == 0) {  // GRAPH Message

			fprintf(stdout, "Received Message: %s\n%s\n", reqframe->command, reqframe->body);
			
			json_t *jneId, *jtimespan, *jtitleX, *jtitleY, *jgroupname;

			jneId = json_object_get(root, "neId");
			jtimespan = json_object_get(root, "timespan");
			jtitleX = json_object_get(root, "titleX");
			jtitleY = json_object_get(root, "titleY");		
			jgroupname = json_object_get(root, "group");
			
			const char *neId = json_string_value(jneId);
			int timespan = json_integer_value(jtimespan);
			const char *titleX = json_string_value(jtitleX);
			const char *titleY = json_string_value(jtitleY);
			const char *groupname = json_string_value(jgroupname);

			// Can we serve this ne_id?
			// extract prefix
			char *prefix_id = strndup(neId, 3);
			// get the NE for this prefix_id
			const NE *ne = get_ne_for_prefix_id(instance->list, prefix_id);

			if (ne == NULL) {  // nope we can't
				fprintf(stderr, "unknown prefix_id %s\n", prefix_id);
				continue;  // TODO: release memory till this point
			}			

			const GROUP *group = get_group_for_group_name(ne, groupname);

			char **calcpr  = NULL;
			int rrdargcount, xsize, ysize, result;
			double ymin, ymax;

			const char **rrdargs;
			int rrdargs_length;
			
			rrdargs_length = 17 + (2 * group->capacity);

			rrdargs = (const char **)calloc(rrdargs_length, sizeof(char *));

			char *rrdfile = NULL;
			asprintf(&rrdfile, "%s/%s.rrd", group->dir, neId);
			
			char *rrdimage = NULL;
			asprintf(&rrdimage, "rrd_images/%s_%s_%d.png", neId, group->name, timespan);

			int i = 0;
			rrdargs[i++] = "rrdgraph";
			rrdargs[i++] = rrdimage;
			rrdargs[i++] = "-a";
			rrdargs[i++] = "PNG";
			
			if (timespan == 0) { // daily
				rrdargs[i++] = "-s";
				rrdargs[i++] = "end-32h";
				rrdargs[i++] = "-x";
				rrdargs[i++] = "HOUR:1:HOUR:4:HOUR:3:0:%H";
			} else if (timespan == 1) { // weekly
				rrdargs[i++] = "-s";
				rrdargs[i++] = "end-8d";
				rrdargs[i++] = "-x";
				rrdargs[i++] = "DAY:1:DAY:2:DAY:1:0:%a";
			} if (timespan == 2) { // monthly
				rrdargs[i++] = "-s";
				rrdargs[i++] = "end-1m";
				rrdargs[i++] = "-x";
				rrdargs[i++] = "DAY:1:DAY:7:DAY:2:0:%d";
			} else if (timespan == 3) {  // yearly
				rrdargs[i++] = "-s";
				rrdargs[i++] = "end-1y";
				// todo check format for the yearly
				rrdargs[i++] = "-x";
				rrdargs[i++] = "DAY:1:DAY:2:DAY:2:0:%d";
			}

			rrdargs[i++] = "-w"; rrdargs[i++] = "400";
			rrdargs[i++] = "-l"; rrdargs[i++] = "0";
			rrdargs[i++] = "--alt-y-grid";
			rrdargs[i++] = "-t"; rrdargs[i++] = titleX;
			rrdargs[i++] = "-v"; rrdargs[i++] = titleY;

			// first the DEFs		
			for (int j = 0; j < group->capacity; j++) {
				RRA *rra = group->rras[j];
				
				char *def = NULL;
				asprintf(&def, "DEF:var%d=%s:%s:AVERAGE", j, rrdfile, rra->name);
				rrdargs[i++] = def;
			}
			
			// now the LINE's
			for (int j = 0; j < group->capacity; j++) {
				RRA *rra = group->rras[j];

				char *line = NULL;
				asprintf(&line, "LINE:var%d#%s:\"%s\"", j, COLORS[j], rra->descr);
				rrdargs[i++] = line;
			}

			// debug graph params
			for (int i = 0; i < rrdargs_length; i++) {
				fprintf(stdout, "%s\n", rrdargs[i]);
			}
			
			//rrd_clear_error();
			
			unsigned long fileLen = 0;
			char *imagebin = NULL;
			
			if (rrd_graph(rrdargs_length, (char **)rrdargs, &calcpr, &xsize, &ysize, NULL, &ymin, &ymax) != -1) {
				 imagebin = read_file_bin(rrdimage, &fileLen);
			} else {
				fprintf(stdout, "Graph error: %s\n", rrd_get_error());
			}
			
			// --------------------------------------------------------------------------------------
			// Send the reply

			json_t *reply = json_object();
			
			fprintf(stdout, "Sending Reply Message:\n");
			{
				stomp_frame frame;
				frame.command = "SEND";
				frame.headers = apr_hash_make(pool);
	
				apr_hash_set(frame.headers, "ServiceRRD_msg_type", APR_HASH_KEY_STRING, "fetchGraphReply");
				// used by the client to receive reply
				apr_hash_set(frame.headers, "ServiceRRD_correlation_id", APR_HASH_KEY_STRING, correlation_id);
				apr_hash_set(frame.headers, "destination", APR_HASH_KEY_STRING, "jms.queue.svc_rrd_reply");


				if (imagebin != NULL) {
					frame.body_length = fileLen;
					frame.body = imagebin;
				} else {
					frame.body_length = -1;
					frame.body = "error creating graph!"; // TODO
				}

				rc = stomp_write(connection, &frame, pool);
				
				if (imagebin != NULL)
					free(imagebin);
					
				rc==APR_SUCCESS || die(-2, "Could not send frame", rc);
			}
	
			json_decref(root);
			json_decref(reply);
		
		}
		
	}

	fprintf(stdout, "Sending Disconnect.");
	{
		stomp_frame frame;
		frame.command = "DISCONNECT";
		frame.headers = NULL;
		frame.body_length = -1;
		frame.body = NULL;
		rc = stomp_write(connection, &frame, pool);
		rc==APR_SUCCESS || die(-2, "Could not send frame", rc);
	}
	fprintf(stdout, "OK\n");
	
	fprintf(stdout, "Disconnecting...");
	rc=stomp_disconnect(&connection);
	rc==APR_SUCCESS || die(-2, "Could not disconnect", rc);
	fprintf(stdout, "OK\n");
	
	apr_pool_destroy(pool);
	return 0;
}

