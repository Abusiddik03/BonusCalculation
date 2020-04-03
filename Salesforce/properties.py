class properties:
    emailList = 'mpansari@godaddy.com'
    # sources = ['EngagementHistory_123', 'EngagementHistory_DF', 'EngagementHistory_HE', 'EngagementHistory_HI', 'EngagementHistory_TSO']
    # basePath = 'hdfs://production/teams/caretech/liveengage/{0}/'
    # targetTables = ['campaign', 'customerinfo', 'customerinfoevent', 'info', 'personalinfoevent', 'postchat', 'visitguids', 'visitorinfo']
    # hiveWarehousePath = "hdfs:///user/hvalluri/warehouse"
    # outputBasePath = "*******"

    # Database and Tables
    # Hive
    auditTableName = "*******"
    hiveDataBaseName = "*******"

    # SQL Server
    maxDBConnections = 4
    JDBCDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    devURL = "jdbc:sqlserver://p3dwsql05.dc1.corp.gd:10504;databaseName=BigReporting;"
    devSchema = "le_emea"
    prodSchema = "LiveEngage"
    dbProperties = {"user": "do-as-datamgmt"
                    , "password": "doasdatamgmtatgodaddy"
                    , "driver": JDBCDriver}
    writeMode = "overwrite"
    dbWriteMode = "overwrite"
    CWHURL = "*******"
    CWHSchema = "*******"
    CWHUser = "*******"
    CWHPwd = "*******"
    auditFlag = True
