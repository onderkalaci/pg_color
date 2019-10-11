
CREATE OR REPLACE FUNCTION color_eq(color, color) 
  RETURNS bool
  LANGUAGE C STRICT
  AS 'pg_color', $$color_eq$$;

CREATE OR REPLACE FUNCTION color_ne(color, color) 
  RETURNS bool
  LANGUAGE C STRICT
  AS 'pg_color', $$color_ne$$;

 CREATE OR REPLACE FUNCTION color_cmp(color, color) 
  RETURNS int
  LANGUAGE C STRICT
  AS 'pg_color', $$color_cmp$$;

CREATE OR REPLACE FUNCTION color_gt(color, color) 
  RETURNS bool
  LANGUAGE C STRICT
  AS 'pg_color', $$color_gt$$;

CREATE OR REPLACE FUNCTION color_ge(color, color) 
  RETURNS bool
  LANGUAGE C STRICT
  AS 'pg_color', $$color_ge$$;

 CREATE OR REPLACE FUNCTION color_lt(color, color) 
  RETURNS bool
  LANGUAGE C STRICT
  AS 'pg_color', $$color_lt$$;

 CREATE OR REPLACE FUNCTION color_le(color, color) 
  RETURNS bool
  LANGUAGE C STRICT
  AS 'pg_color', $$color_le$$;



CREATE OPERATOR = (
	LEFTARG = color,
	RIGHTARG = color,
	PROCEDURE = color_eq,
	COMMUTATOR = '=',
	NEGATOR = '<>',
	RESTRICT = eqsel,
	JOIN = eqjoinsel
);
COMMENT ON OPERATOR =(color, color) IS 'equals?';

CREATE OPERATOR <> (
	LEFTARG = color,
	RIGHTARG = color,
	PROCEDURE = color_ne,
	COMMUTATOR = '<>',
	NEGATOR = '=',
	RESTRICT = neqsel,
	JOIN = neqjoinsel
);
COMMENT ON OPERATOR <>(color, color) IS 'not equals?';


CREATE OPERATOR > (
	LEFTARG = color,
	RIGHTARG = color,
	PROCEDURE = color_gt,
	NEGATOR = '=',
	RESTRICT = neqsel,
	JOIN = neqjoinsel
);
COMMENT ON OPERATOR <>(color, color) IS 'not equals?';

CREATE OPERATOR >= (
	LEFTARG = color,
	RIGHTARG = color,
	PROCEDURE = color_ge,
	NEGATOR = '=',
	RESTRICT = neqsel,
	JOIN = neqjoinsel
);
COMMENT ON OPERATOR <>(color, color) IS 'not equals?';


CREATE OPERATOR < (
	LEFTARG = color,
	RIGHTARG = color,
	PROCEDURE = color_lt,
	NEGATOR = '=',
	RESTRICT = neqsel,
	JOIN = neqjoinsel
);
COMMENT ON OPERATOR <>(color, color) IS 'not equals?';

CREATE OPERATOR <= (
	LEFTARG = color,
	RIGHTARG = color,
	PROCEDURE = color_le,
	NEGATOR = '=',
	RESTRICT = neqsel,
	JOIN = neqjoinsel
);
COMMENT ON OPERATOR <>(color, color) IS 'not equals?';




