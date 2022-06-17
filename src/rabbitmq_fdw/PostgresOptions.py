
def getPostgresOptions(plpy, servername):
    import json
    def getDictFromOptions(opt):
        options = {}
        for i in opt:
            (key , value) = i.split('=')
            options[key]= value
        return options 
    serveroptions = plpy.execute("""
        select fs.srvoptions
        from pg_foreign_server fs
        join pg_foreign_data_wrapper fdw on fdw.oid=fs.srvfdw
        where fdw.fdwname='multicorn' and fs.srvname='%s'
    """ % servername)
    if serveroptions.nrows()==0: 
        raise Exception("Empty options for provided server name")
    serveroptions = getDictFromOptions(serveroptions[0]['srvoptions'])
    credentials =  plpy.execute("""
        select um.umoptions
        from pg_foreign_server fs
                    join pg_foreign_data_wrapper fdw on fdw.oid = fs.srvfdw
                    join pg_user_mappings um on um.srvname = fs.srvname
        where fs.srvname = '%s'
        and fdw.fdwname = 'multicorn'
        limit 1 
    """ % servername)
    if credentials.nrows()==0: 
        raise Exception("Empty credentials for provided server name")
    credentials = getDictFromOptions(credentials[0]['umoptions'])
    result = {}
    result.update(serveroptions)
    result.update(credentials)
    return result

 