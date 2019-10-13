/*
 * pg_color: Color data type for PostgreSQL
 *
 * Author: Burak Yucesoy <burak@citusdata.com>
 */

#include "postgres.h"

#include "utils/elog.h"
#include "utils/palloc.h"
#include "utils/builtins.h"
#include "libpq/pqformat.h"

#ifndef PG_VERSION_NUM
#error "Unsupported too old PostgreSQL version"
#endif

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

void _PG_init(void);

typedef struct color
{
	uint8 r;
	uint8 g;
	uint8 b;
} color;


#define DatumGetColor(X)	 ((color *) DatumGetPointer(X))
#define ColorGetDatum(X)	 PointerGetDatum(X)
#define PG_GETARG_COLOR(n)	 DatumGetColor(PG_GETARG_DATUM(n))
#define PG_RETURN_COLOR(x)	 return ColorGetDatum(x)

#include "miscadmin.h"
#include "optimizer/planner.h"
#include "nodes/extensible.h"

PlannedStmt *
pg_color_planner(Query *parse, int cursorOptions, ParamListInfo boundParams);
static Node *
PgColorCreateScan(CustomScan *scan);
CustomScanMethods PgColorScanMethods = {
	"PGColor Scan",
	PgColorCreateScan
};
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(DEBUG4, (errmsg("Citus can only be loaded via shared_preload_libraries"),
						errhint("Add citus to shared_preload_libraries configuration "
								"variable in postgresql.conf in master and workers. Note "
								"that citus should be at the beginning of "
								"shared_preload_libraries.")));
	}

	RegisterCustomScanMethods(&PgColorScanMethods);

	/* intercept planner */
	planner_hook = pg_color_planner;
}
#include "nodes/extensible.h"
typedef struct PGColorScanState
{
	CustomScanState customScanState;  /* underlying custom scan node */

	Const *data;

} PGColorScanState;




static void
PgColorBeginScan(CustomScanState *node, EState *estate, int eflags);
static CustomExecMethods AdaptiveExecutorCustomExecMethods = {
	.CustomName = "PGColorScanMethod",
	.BeginCustomScan = PgColorBeginScan
};


static void
PgColorBeginScan(CustomScanState *node, EState *estate, int eflags)
{
	PGColorScanState *scanState = NULL;


	scanState = (PGColorScanState *) node;
	elog(INFO, "PgColorBeginScan");

}

/*
 * AdaptiveExecutorCreateScan creates the scan state for the adaptive executor.
 */
static Node *
PgColorCreateScan(CustomScan *scan)
{
	PGColorScanState *scanState = palloc0(sizeof(PGColorScanState));

	scanState->customScanState.ss.ps.type = T_CustomScanState;

	scanState->customScanState.methods = &AdaptiveExecutorCustomExecMethods;


	Node *node = (Node *) linitial(scan->custom_private);
	Assert(IsA(node, Const));

	scanState->data =  (Const *) node;

	elog(INFO, "PgColorCreateScan: %ld", DatumGetInt64(((Const *) node)->constvalue));


	return (Node *) scanState;
}


#include "nodes/makefuncs.h"
PlannedStmt *
pg_color_planner(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
	CustomScan *customScan = makeNode(CustomScan);
	static int scanCount = 1;

	++scanCount;
	elog(INFO, "Intercepted the planner");

	Const *c = (Const *) makeConst(20, -1, InvalidOid,
			   sizeof(int64),
			   Int64GetDatum(scanCount), false,
			   FLOAT8PASSBYVAL);
	PlannedStmt *result = standard_planner(parse, cursorOptions, boundParams);
	Node *PgColorData = NULL;

	//customScan->scan.plan.targetlist = copyObject(parse->targetList);


	customScan->methods = &PgColorScanMethods;

	PgColorData = (Node *) c;

	customScan->custom_private = list_make1(PgColorData);
	customScan->flags = CUSTOMPATH_SUPPORT_BACKWARD_SCAN;

	result->planTree =  &customScan->scan.plan;

	return  result;
}


/* RemoteScanRangeTableEntry creates a range table entry from given column name
* list to represent a remote scan.
*/
RangeTblEntry *
RemoteScanRangeTableEntry(List *columnNameList)
{
	RangeTblEntry *remoteScanRangeTableEntry = makeNode(RangeTblEntry);

	/* we use RTE_VALUES for custom scan because we can't look up relation */
	remoteScanRangeTableEntry->rtekind = RTE_VALUES;
	remoteScanRangeTableEntry->eref = makeAlias("remote_scan", columnNameList);
	remoteScanRangeTableEntry->inh = false;
	remoteScanRangeTableEntry->inFromCl = true;

	return remoteScanRangeTableEntry;
}

static inline
color * color_from_str(char *str)
{
  color *c = palloc0(sizeof(color));


	char *endptr = NULL;
	char *cur = str;
	if (cur[0] != '(')
		elog(ERROR, "expected '(' at position 0");

	cur++;
	c->r = strtol(cur, &endptr, 10);
	if (cur == endptr)
		elog(ERROR, "expected number at position 1");
	if (endptr[0] != ',')
		elog(ERROR, "expected ',' at position " INT64_FORMAT, endptr - str);


	cur = endptr + 1;
	c->g = strtoll(cur, &endptr, 10);

	if (cur == endptr)
		elog(ERROR, "expected number at position 2");
	if (endptr[0] != ',')
		elog(ERROR, "expected ',' at position " INT64_FORMAT, endptr - str);

	cur = endptr + 1;
	c->b = strtoll(cur, &endptr, 10);

	if (endptr[0] != ')')
		elog(ERROR, "expected ')' at position " INT64_FORMAT, endptr - str);
	if (endptr[1] != '\0')
		elog(ERROR, "unexpected character at position " INT64_FORMAT, 1 + (endptr - str));


  return c;
}

static inline
char *color_to_str(color *c)
{
	char *s = psprintf("(%d,%d,%d)", c->r, c->g, c->b);
  return s;
}

Datum color_in(PG_FUNCTION_ARGS);
Datum color_out(PG_FUNCTION_ARGS);

Datum color_eq(PG_FUNCTION_ARGS);
Datum color_ne(PG_FUNCTION_ARGS);
Datum color_cmp(PG_FUNCTION_ARGS);

Datum color_lt(PG_FUNCTION_ARGS);
Datum color_le(PG_FUNCTION_ARGS);
Datum color_gt(PG_FUNCTION_ARGS);
Datum color_ge(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(color_in);
Datum
color_in(PG_FUNCTION_ARGS)
{
    char *str = PG_GETARG_CSTRING(0);

    PG_RETURN_COLOR(color_from_str(str));
}

PG_FUNCTION_INFO_V1(color_out);
Datum
color_out(PG_FUNCTION_ARGS)
{
  color *c = (color *) PG_GETARG_COLOR(0);

  PG_RETURN_CSTRING(color_to_str(c));
}


PG_FUNCTION_INFO_V1(color_eq);
Datum
color_eq(PG_FUNCTION_ARGS)
{

  color *c1 = PG_GETARG_COLOR(0);
  color *c2 = PG_GETARG_COLOR(1);

 // return 0;

  return c1->r == c2->r && c1->g == c2->g && c1->b == c2->b;
}

PG_FUNCTION_INFO_V1(color_ne);
Datum
color_ne(PG_FUNCTION_ARGS)
{
  color *c1 = (color *) PG_GETARG_COLOR(0);
  color *c2 = (color *) PG_GETARG_COLOR(1);

  return c1->r != c2->r || c1->g != c2->g || c1->b != c2->b;
}


PG_FUNCTION_INFO_V1(color_cmp);
Datum
color_cmp(PG_FUNCTION_ARGS)
{
  color *c1 = (color *) PG_GETARG_COLOR(0);
  color *c2 = (color *) PG_GETARG_COLOR(1);

  if (c1 == NULL)
	  return 1;

  if (c2 == NULL)
	  return -1;

  if (c1->r > c2->r)
	  return 1;
  else  if (c1->r < c2->r)
	  return -1;

  if (c1->g > c2->g)
	  return 1;
  else  if (c1->g < c2->g)
	  return -1;

  if (c1->b > c2->b)
	  return 1;
  else  if (c1->b < c2->b)
	  return -1;

  return 0;
}


PG_FUNCTION_INFO_V1(rgb_distance);
Datum
rgb_distance(PG_FUNCTION_ARGS)
{
  color *c1 = (color *) PG_GETARG_COLOR(0);
  color *c2 = (color *) PG_GETARG_COLOR(1);


  double d1 = (double)c1->r - c2->r;
  double d2 = (double)c1->g - c2->g;
  double d3 = (double)c1->b - c2->b;


  PG_RETURN_FLOAT8(sqrt(d1 * d1 + d2 * d2 + d3 * d3));
}

PG_FUNCTION_INFO_V1(color_lt);
Datum
color_lt(PG_FUNCTION_ARGS)
{
  color *c1 = (color *) PG_GETARG_COLOR(0);
  color *c2 = (color *) PG_GETARG_COLOR(1);

  if (c1->r < c2->r)
	  return 1;
  else if (c1->r > c2->r)
	  return 0;

  if (c1->g < c2->g)
	  return 1;
  else if (c1->g > c2->g)
	  return 0;

  if (c1->b < c2->b)
	  return 1;
  else if (c1->b > c2->b)
	  return 0;

  return 0;
}


PG_FUNCTION_INFO_V1(color_le);
Datum
color_le(PG_FUNCTION_ARGS)
{
  color *c1 = (color *) PG_GETARG_COLOR(0);
  color *c2 = (color *) PG_GETARG_COLOR(1);

  if (c1->r < c2->r)
	  return 1;
  else if (c1->r > c2->r)
	  return 0;

  if (c1->g < c2->g)
	  return 1;
  else if (c1->g > c2->g)
	  return 0;

  if (c1->b < c2->b)
	  return 1;
  else if (c1->b > c2->b)
	  return 0;

  return 1;
}

PG_FUNCTION_INFO_V1(color_gt);
Datum
color_gt(PG_FUNCTION_ARGS)
{
  color *c1 = (color *) PG_GETARG_COLOR(0);
  color *c2 = (color *) PG_GETARG_COLOR(1);

  if (c1->r > c2->r)
	  return 1;
  else if (c1->r < c2->r)
	  return 0;

  if (c1->g > c2->g)
	  return 1;
  else if (c1->g < c2->g)
	  return 0;

  if (c1->b > c2->b)
	  return 1;
  else if (c1->b < c2->b)
	  return 0;

	 return 0;
}


PG_FUNCTION_INFO_V1(color_ge);
Datum
color_ge(PG_FUNCTION_ARGS)
{
  color *c1 = (color *) PG_GETARG_COLOR(0);
  color *c2 = (color *) PG_GETARG_COLOR(1);

  if (c1->r > c2->r)
	  return 1;
  else if (c1->r < c2->r)
	  return 0;

  if (c1->g > c2->g)
	  return 1;
  else if (c1->g < c2->g)
	  return 0;

  if (c1->b > c2->b)
	  return 1;
  else if (c1->b < c2->b)
return 0;

	 return 1;
}

PG_FUNCTION_INFO_V1(color_send);

Datum
color_send(PG_FUNCTION_ARGS)
{
	color *a = PG_GETARG_COLOR(0);
	StringInfoData buf;

	pq_begintypsend(&buf);

	pq_sendint8(&buf, a->r);
	pq_sendint8(&buf, a->g);
	pq_sendint8(&buf, a->b);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

PG_FUNCTION_INFO_V1(color_recv);

Datum
color_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

	color *result = palloc0(sizeof(color));
	result->r = pq_getmsgint64(buf);
	result->g = pq_getmsgint64(buf);
	result->b = pq_getmsgint64(buf);

	PG_RETURN_COLOR(result);
}
