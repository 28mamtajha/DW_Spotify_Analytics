select
    track_id,
    track_name,
    artists,
    album_name,
    release_date,
    popularity,
    duration_ms,
    explicit
from {{ source('spotify', 'REALTIME_TRENDING_TRACKS') }}
