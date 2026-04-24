BEGIN;

-- -------------------------------- job group and registry --------------------------------

CREATE TABLE xxl_job_group (
    id           bigserial NOT NULL,
    app_name     varchar(64) NOT NULL,
    title        varchar(12) NOT NULL,
    address_type smallint NOT NULL DEFAULT 0,
    address_list text,
    update_time  timestamp(0) DEFAULT NULL,
    PRIMARY KEY (id)
);

COMMENT ON TABLE xxl_job_group IS '执行器分组表';
COMMENT ON COLUMN xxl_job_group.app_name IS '执行器AppName';
COMMENT ON COLUMN xxl_job_group.title IS '执行器名称';
COMMENT ON COLUMN xxl_job_group.address_type IS '执行器地址类型：0=自动注册、1=手动录入';
COMMENT ON COLUMN xxl_job_group.address_list IS '执行器地址列表，多地址逗号分隔';

CREATE TABLE xxl_job_registry (
    id             bigserial NOT NULL,
    registry_group varchar(50)  NOT NULL,
    registry_key   varchar(255) NOT NULL,
    registry_value varchar(255) NOT NULL,
    update_time    timestamp(0) DEFAULT NULL,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX i_g_k_v ON xxl_job_registry (registry_group, registry_key, registry_value);

-- -------------------------------- job info --------------------------------

CREATE TABLE xxl_job_info (
    id                        bigserial NOT NULL,
    job_group                 bigint NOT NULL,
    job_desc                  varchar(255) NOT NULL,
    add_time                  timestamp(0) DEFAULT NULL,
    update_time               timestamp(0) DEFAULT NULL,
    author                    varchar(64) DEFAULT NULL,
    alarm_email               varchar(255) DEFAULT NULL,
    schedule_type             varchar(50) NOT NULL DEFAULT 'NONE',
    schedule_conf             varchar(128) DEFAULT NULL,
    misfire_strategy          varchar(50) NOT NULL DEFAULT 'DO_NOTHING',
    executor_route_strategy   varchar(50) DEFAULT NULL,
    executor_handler          varchar(255) DEFAULT NULL,
    executor_param            varchar(512) DEFAULT NULL,
    executor_block_strategy   varchar(50) DEFAULT NULL,
    executor_timeout          integer NOT NULL DEFAULT 0,
    executor_fail_retry_count integer NOT NULL DEFAULT 0,
    glue_type                 varchar(50) NOT NULL,
    glue_source               text,
    glue_remark               varchar(128) DEFAULT NULL,
    glue_updatetime           timestamp(0) DEFAULT NULL,
    child_jobid               varchar(255) DEFAULT NULL,
    trigger_status            smallint NOT NULL DEFAULT 0,
    trigger_last_time         bigint NOT NULL DEFAULT 0,
    trigger_next_time         bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (id)
);

COMMENT ON COLUMN xxl_job_info.job_group IS '执行器主键ID';
COMMENT ON COLUMN xxl_job_info.trigger_status IS '调度状态：0-停止，1-运行';

CREATE TABLE xxl_job_logglue (
    id          bigserial NOT NULL,
    job_id      bigint NOT NULL,
    glue_type   varchar(50) DEFAULT NULL,
    glue_source text,
    glue_remark varchar(128) NOT NULL,
    add_time    timestamp(0) DEFAULT NULL,
    update_time timestamp(0) DEFAULT NULL,
    PRIMARY KEY (id)
);

-- -------------------------------- job log and report --------------------------------

CREATE TABLE xxl_job_log (
    id                        bigserial NOT NULL,
    job_group                 bigint NOT NULL,
    job_id                    bigint NOT NULL,
    executor_address          varchar(255) DEFAULT NULL,
    executor_handler          varchar(255) DEFAULT NULL,
    executor_param            varchar(512) DEFAULT NULL,
    executor_sharding_param   varchar(20) DEFAULT NULL,
    executor_fail_retry_count integer NOT NULL DEFAULT 0,
    trigger_time              timestamp(0) DEFAULT NULL,
    trigger_code              integer NOT NULL,
    trigger_msg               text,
    handle_time               timestamp(0) DEFAULT NULL,
    handle_code               integer NOT NULL,
    handle_msg                text,
    alarm_status              smallint NOT NULL DEFAULT 0,
    PRIMARY KEY (id)
);

CREATE INDEX I_trigger_time ON xxl_job_log (trigger_time);
CREATE INDEX I_handle_code ON xxl_job_log (handle_code);
CREATE INDEX I_jobid_jobgroup ON xxl_job_log (job_id, job_group);
CREATE INDEX I_job_id ON xxl_job_log (job_id);

CREATE TABLE xxl_job_log_report (
    id            bigserial NOT NULL,
    trigger_day   timestamp(0) DEFAULT NULL,
    running_count integer NOT NULL DEFAULT 0,
    suc_count     integer NOT NULL DEFAULT 0,
    fail_count    integer NOT NULL DEFAULT 0,
    update_time   timestamp(0) DEFAULT NULL,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX i_trigger_day ON xxl_job_log_report (trigger_day);

-- -------------------------------- lock --------------------------------

CREATE TABLE xxl_job_lock (
    lock_name varchar(50) NOT NULL,
    PRIMARY KEY (lock_name)
);

INSERT INTO xxl_job_lock (lock_name) VALUES ('schedule_lock');

-- -------------------------------- user --------------------------------

CREATE TABLE xxl_job_user (
    id         bigserial NOT NULL,
    username   varchar(50) NOT NULL,
    password   varchar(100) NOT NULL,
    token      varchar(100) DEFAULT NULL,
    role       smallint NOT NULL,
    permission varchar(255) DEFAULT NULL,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX i_username ON xxl_job_user (username);

-- -------------------------------- for default data --------------------------------

INSERT INTO xxl_job_group(id, app_name, title, address_type, address_list, update_time)
VALUES (1, 'xxl-job-executor-sample', '通用执行器Sample', 0, NULL, CURRENT_TIMESTAMP),
       (2, 'xxl-job-executor-sample-ai', 'AI执行器Sample', 0, NULL, CURRENT_TIMESTAMP);

INSERT INTO xxl_job_info(id, job_group, job_desc, add_time, update_time, author, alarm_email, schedule_type, schedule_conf, misfire_strategy, executor_route_strategy, executor_handler, executor_param, executor_block_strategy, executor_timeout, executor_fail_retry_count, glue_type, glue_source, glue_remark, glue_updatetime, child_jobid)
VALUES (1, 1, '示例任务01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'XXL', '', 'CRON', '0 0 0 * * ? *', 'DO_NOTHING', 'FIRST', 'demoJobHandler', '', 'SERIAL_EXECUTION', 0, 0, 'BEAN', '', 'GLUE代码初始化', CURRENT_TIMESTAMP, ''),
       (2, 2, 'Ollama示例任务01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'XXL', '', 'NONE', '', 'DO_NOTHING', 'FIRST', 'ollamaJobHandler', '{"input": "慢SQL问题分析思路", "prompt": "你是一个研发工程师，擅长解决技术类问题。", "model": "qwen3:0.6b"}', 'SERIAL_EXECUTION', 0, 0, 'BEAN', '', 'GLUE代码初始化', CURRENT_TIMESTAMP, ''),
       (3, 2, 'Dify示例任务', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'XXL', '', 'NONE', '', 'DO_NOTHING', 'FIRST', 'difyWorkflowJobHandler', '{"inputs":{"input":"查询班级各学科前三名"}, "user": "xxl-job", "baseUrl": "http://localhost/v1", "apiKey": "app-OUVgNUOQRIMokfmuJvBJoUTN"}', 'SERIAL_EXECUTION', 0, 0, 'BEAN', '', 'GLUE代码初始化', CURRENT_TIMESTAMP, '');

INSERT INTO xxl_job_user(id, username, password, role, permission)
VALUES (1, 'admin', '8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92', 1, NULL);

-- Sync sequences
SELECT setval('xxl_job_group_id_seq', (SELECT MAX(id) FROM xxl_job_group));
SELECT setval('xxl_job_info_id_seq', (SELECT MAX(id) FROM xxl_job_info));
SELECT setval('xxl_job_user_id_seq', (SELECT MAX(id) FROM xxl_job_user));

COMMIT;
