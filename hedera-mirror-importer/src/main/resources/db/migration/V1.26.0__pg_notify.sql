-- Define trigger function. Base64 encoding is required since JSON doesn't support binary
create or replace function topic_message_notifier()
    returns trigger
    language plpgsql
as
$$
declare
    topicmsg text := TG_ARGV[0];
begin
    perform (
        with payload(consensus_timestamp, realm_num, topic_num, message, running_hash, running_hash_version, sequence_number) as
                 (
                     select NEW.consensus_timestamp,
                            NEW.realm_num,
                            NEW.topic_num,
                            encode(NEW.message, 'base64'),
                            encode(NEW.running_hash, 'base64'),
                            NEW.running_hash_version,
                            NEW.sequence_number
                 )
        select pg_notify(topicmsg, row_to_json(payload)::text)
        from payload
    );
    return null;
end;
$$;

-- Setup trigger
create trigger topic_message_trigger
    after insert
    on topic_message
    for each row
execute procedure topic_message_notifier('topic_message');
commit;
