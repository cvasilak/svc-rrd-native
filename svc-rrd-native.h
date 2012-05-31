#ifndef SVC_RRD_NATIVE_H
#define SVC_RRD_NATIVE_H

typedef struct {
	const char *name;
	const char *type;
	const char *descr;
} RRA;

typedef struct {
	const char *name;
	const char *dir;
	int capacity;	
	RRA  **rras;
} GROUP;

typedef struct {
	const char *prefix_id;
	int capacity;
	GROUP **groups;
} NE;

typedef struct {
	int capacity;
	NE **ne;
} NES;

typedef struct {
	const char *instance_id;  		// the UIID of this instance
	const char *listening_queue;	// the listening queue of this instance
	NES  *list;						// the network elements that this instance monitors
} INSTANCE;


const NE *get_ne_for_prefix_id(const NES **list, char *prefix_id);

const GROUP *get_group_for_group_name(const NE *ne, const char *name);

const GROUP *get_group_for_rra_name(const NE *ne, const char *name);

const char *COLORS[] = {"0000FF", "228B22", "FF5700", "000000", NULL};

#endif






