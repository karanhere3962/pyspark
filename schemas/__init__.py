from pydantic import BaseModel, conint


class SQLAPISchema(BaseModel):

    sql_query: str
    offset: conint(ge=0) = 0
    limit: conint(ge=0, le=200) = 200

