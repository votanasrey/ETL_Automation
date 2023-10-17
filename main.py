import sql_function

def __app__():
    if sql_function.__checkexist__() == 1:
        if sql_function.__columnsame__() == 1:
            sql_function.__delete__();
            sql_function.__ingest__();
            sql_function.__copy2remove__();
            return "data ingestion complete"
        else:
            return sql_function.__columnsame__()
    else:
        if sql_function.__spec2data__() ==1:
            sql_function.__create__();
            sql_function.__ingest__();
            sql_function.__copy2remove__();
            return "table creation and data ingestion complete"
        else:
            return sql_function.__spec2data__();

if __name__ == '__main__':
    print(__app__())
