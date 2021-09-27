# sqlakeyset: modified version using 2.0-style ORM queries and asyncio
 
This fork contains a heavily modified version of the [sqlakeyset](https://github.com/djrobstep/sqlakeyset) library which
uses SQLAlchemy 2.0-style ORM queries and asyncio.

Notes:
1. This version of the library was written to work exclusively with 2.0 style over asyncio, so all 1.3/1.4 related code
was removed completely.
2. The code here does not run as-is. Minimal integration is required, depends on how a DB session is acquired in your
codebase.

See the [relevant issue](https://github.com/djrobstep/sqlakeyset/issues/54).

