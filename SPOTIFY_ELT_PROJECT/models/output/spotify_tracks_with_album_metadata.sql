with tracks as (
    select * from {{ ref('spotify_tracks_all') }}
),

albums as (
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
    from {{ ref('spotify_top_albums_staging') }}
)

select
    t.*,
    a.album_id,
    a.artist_id,
    a.artist_name,
    a.release_date as album_release_date,
    a.album_type,
    a.num_tracks,
    a.external_url,
    a.image_url
from tracks t
left join albums a
  on lower(t.album_name) = lower(a.album_name)

