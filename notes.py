notes = {}

notes[1] = [
    "PostgreSQL connections consume memory and CPU resources even when idle. As queries are run on a connection, memory gets allocated."
    + "This memory is not completely freed up even when the connection goes idle.If your application is configured in a way that results in idle connections",
    "it is recommended to use a connection pooler so your memory and CPU resources aren’t wasted just to manage the idle connection",
]

notes[4] = [
    "Looking at the access pattern, consider partitioning large tables for improved query performance, reducing IOs, easier data purge and better autovacuum performance",
    'Reference: <a href="https://www.postgresql.org/docs/current/ddl-partitioning.html" target="_blank">Partitioning</a>',
]

notes[5] = [
    "For better write performance, saving write IOs, saving storage, look at the index definitions and consider dropping duplicate indexes"
]

notes[6] = [
    "For better write performance, saving write IOs, saving storage, look at the index definitions and consider dropping unused indexes"
]

notes[7] = [
    "Reference: Please visit https://www.postgresql.org/docs/current/routine-vacuuming.html doc for more details on database age"
]

notes[8] = [
    "Consider VACUUM highly bloated tables during off peak hours",
    "Use RDS/Aurora supported pg_cron extension to schedule manual VACUUM job",
    "Consider recreating bloated indexes. Use pg_repack extension to reclaim space",
]

notes[9] = [
    "If large tables are not autovacuumed recently, consider logging autovacuum activities. Consider VACCUM these tables during off peak hours"
]

notes[12] = [
    "Run EXPLAIN plan on the top CPU consuming queries and optimize",
    'Please watch this <a href=https://www.youtube.com/watch?v=XKPHbYe-fHQ target="_blank">AWS video</a> on RDS/Aurora PostgreSQL query tuning',
    "This AWS knowledge-center article discusses about troubleshooting high CPU issue at RDS/Aurora PostgreSQL",
]

notes[13] = [
    'Run EXPLAIN plan on the top 10 Read queries with low "hit_percent"',
    "Focus on proper indexing, partitioning, checkpoints and VACUUM ANALYZE on heavily used tables",
    'Look at CloudWatch metric "BufferCacheHitRatio". Lower the value, higher the Read IO cost',
]

notes[14] = [
    "For better VACUUM and ANALYZE performance on tables with high bloat and high UPDATE/DELETE operations, modify autovacuum parameters on table level",
    "Changing parameters on instance level will impact all tables in the databases",
]

notes[15] = [
    'Pick the tables with low "hit_percent" and consider about partitioning, optimizing queries related to those tables',
    "Also consider using pg_prewarm extension to load relation data into buffer cache",
]

notes[16] = [
    "Consider enabling log_min_duration_statement to log slow running queries. Value of 5000 logs queries running more than for 5 seconds",
    'Enabling "log_statement" paramter logs none (off), ddl, mod, or all statements',
    "Verbose logging can increase the log file sizes",
    'Please visit this <a href="https://aws.amazon.com/blogs/database/working-with-rds-and-aurora-postgresql-logs-part-1/" target="_blank">AWS blog</a>'
    + " for more details on working with RDS/Aurora logs",
    'this <a href="https://aws.amazon.com/blogs/database/part-2-audit-aurora-postgresql-databases-using-database-activity-streams-and-pgaudit/?nc1=b_nrp" target="_blank">AWS blog</a>'
    + ' for more details on Aurora PostgreSQL Database <a href="https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/DBActivityStreams.Overview.html" target="_blank">Activity Streams</a> and pgAudit',
    "Enable log_temp_files to log temporary files names and sizes",
]

notes[17] = [
    'Please visit "<a href="https://aws.amazon.com/blogs/database/amazon-aurora-postgresql-parameters-part-3-optimizer-parameters/" target="_blank">this AWS blog</a>'
    + " for more details on Aurora PostgreSQL optimizer parameters"
]

notes[18] = [
    "Please visit PostgreSQL doc for more details on table age. Consider VACUUM FREEZE on the highly aged tables"
]
