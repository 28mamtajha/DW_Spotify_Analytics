select
    track_id,
    track_name,
    artists,
    album_name,
    release_date,
    duration_ms,
    explicit,
    popularity
from {{ source('spotify', 'REALTIME_NEW_RELEASES') }}

