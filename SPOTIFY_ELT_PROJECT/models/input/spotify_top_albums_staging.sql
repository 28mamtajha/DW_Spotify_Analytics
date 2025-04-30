select
    album_id,
    album_name,
    artist_id,
    artist_name,
    release_date,
    album_type,
    num_tracks,
    external_url,
    image_url
from {{ source('spotify', 'REALTIME_TOP_ALBUMS') }}

