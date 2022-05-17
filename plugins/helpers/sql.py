class SqlQueries:


    create_event = """
        CREATE TABLE IF NOT EXISTS nikolay.event
        (
            id                      varchar(256) not null,
            created_at              timestamp,
            device_id               varchar(256),
            ip_address              varchar(256),
            user_id                 varchar(256),
            uuid                    varchar(256),
            event_type              varchar(256),
            platform                varchar(256),
            device_type             varchar(256),
            event_source            varchar(256),
            ep_questiontype         varchar(256),
            ep_session_id           bigint,
            ep_screen               varchar(256),
            ep_from_background      boolean,
            ep_label                varchar(256),
            user_email              varchar(256),
            meta_browser_user_agent varchar(256),
            meta_page_origin        varchar(256),
            meta_page_path          varchar(256),
            meta_page_title         varchar(256),
            meta_page_url           varchar(256),
            meta_page_search        varchar(256),
            insert_dtime            timestamp default SYSDATE,
            primary key (id)
        )
            distkey (id)
            sortkey (created_at)
        ;
        """

    create_event_sequence = """
        CREATE TABLE nikolay.event_sequence
        (
            event_id          varchar(256) not null,
            user_id           varchar(256),
            event_previous_id varchar(256),
            event_next_id     varchar(256),
            insert_dtime      timestamp default SYSDATE
        )
        sortkey (user_id)
        distkey (event_id)
        ;
        """

    delete_increment_from_target = """
        DELETE
        FROM {}
        WHERE created_at::DATE BETWEEN '{}' and '{}'
        ;
        """

    check_increment_loaded_to_target = """
        SELECT id
        FROM {}
        WHERE id IN (SELECT id FROM {}_inc)
        ;
    """

    insert_increment_to_event = """
        INSERT INTO {}
        SELECT id,
               created_at,
               device_id,
               ip_address,
               user_id,
               uuid,
               event_type,
               platform,
               device_type,
               event_source,
               ep_questiontype,
               ep_session_id,
               ep_screen,
               ep_from_background,
               ep_label,
               user_email,
               meta_browser_user_agent,
               meta_page_origin,
               meta_page_path,
               meta_page_title,
               meta_page_url,
               meta_page_search
        from {}_inc
        ;
        """

    insert_events_to_event_sequence = """
        INSERT INTO nikolay.event_sequence
        SELECT ev.id AS event_id,
               ev.user_id AS user_id,
               LAG(ev.id) OVER (PARTITION BY ev.user_id ORDER BY ev.created_at)  AS event_previous_id,
               LEAD(ev.id) OVER (PARTITION BY ev.user_id ORDER BY ev.created_at) AS event_next_id
        FROM nikolay.event ev
        WHERE ev.user_id IS NOT NULL
        ;
        """