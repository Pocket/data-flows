select 
{% if for_new_offset %}
    max(collector_tstamp) as new_offset
{% else %}
collector_tstamp as submission_timestamp,
derived_tstamp as event_timestamp,
app_id as app_id,
object_construct(
    'geo', object_construct(
      'city', geo_city, 
      'country', geo_country),
    'user_agent', object_construct(
      'browser', CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_UA_PARSER_CONTEXT_1[0]:useragentFamily::string, 
      'os', CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_UA_PARSER_CONTEXT_1[0]:osFamily::string,
      'version', CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_UA_PARSER_CONTEXT_1[0]:useragentMajor::string || CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_UA_PARSER_CONTEXT_1[0]:useragentMinor::string)
    ) as metadata,
object_construct(
    'app_build', nvl(CONTEXTS_COM_SNOWPLOWANALYTICS_MOBILE_APPLICATION_1[0]:version::string, CONTEXTS_COM_POCKET_API_USER_1[0]:client_version::string),
    'client_id', CONTEXTS_COM_POCKET_USER_1[0]:hashed_guid::string,
    'user_id', CONTEXTS_COM_POCKET_USER_1[0]:hashed_user_id::string,
    'locale', br_lang,
    'os', CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_UA_PARSER_CONTEXT_1[0]:osFamily::string,
    'os_version', CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_UA_PARSER_CONTEXT_1[0]:osMajor::string || CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_UA_PARSER_CONTEXT_1[0]:osMinor::string
    ) as client_info,
'event' as event_category,
event_name,
object_construct(
    'api_id', COALESCE(CONTEXTS_COM_POCKET_API_USER_1[0]:api_id::int, 0),
    'page_url_host', page_urlhost,
    'page_url_path', page_urlpath,
    'page_url_query', page_urlquery,
    'bottom_ui_type', CONTEXTS_COM_POCKET_UI_1[0]:type::string,
    'bottom_ui_component_detail', CONTEXTS_COM_POCKET_UI_1[0]:component_detail::string,
    'bottom_ui_identifier', CONTEXTS_COM_POCKET_UI_1[0]:identifier::string,
    'bottom_ui_index', CONTEXTS_COM_POCKET_UI_1[0]:index::integer
) as event_extras
{% endif %}
from {{ database_name }}.{{ schema_name }}.{{ table_name }}
where collector_tstamp >= '{{ batch_start }}'
and collector_tstamp < '{{ batch_end }}'
and (
    (event_name = 'list_item_update' and UNSTRUCT_EVENT_COM_POCKET_LIST_ITEM_UPDATE_1:trigger::string = 'save')
    or
    (event_name = 'content_open')
    or
    (event_name = 'app_open')
    or
    (event_name = 'page_view' and
        (
        CONTEXTS_COM_POCKET_USER_1[0]:hashed_user_id::string is not null
        or page_urlpath like any ('/explore%' , '/__/explore%', '/__-__/explore%')
        or page_urlpath like any ('/collections%' , '/__/collections%', '/__-__/collections%')
        )
    )
    and app_id not like '%-dev' 
    and app_id like 'pocket-%'
){% if for_new_offset %}
{% else %}
qualify row_number () over (partition by event_id order by collector_tstamp) = 1
{% endif %}

