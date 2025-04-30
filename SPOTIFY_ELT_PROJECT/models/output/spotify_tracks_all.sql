with historical as (
    select
        track_id,
        track_name,
        artists,
        album_name,
        popularity,
        duration_ms,
        explicit,
        danceability,
        energy,
        speechiness,
        acousticness,
        instrumentalness,
        liveness,
        valence,
        tempo,
        track_genre,
        'historical' as source_type
    from {{ ref('spotify_historical_tracks_staging') }}
),

new_releases as (
    select
        track_id,
        track_name,
        artists,
        album_name,
        popularity,
        duration_ms,
        explicit,
        null as danceability,
        null as energy,
        null as speechiness,
        null as acousticness,
        null as instrumentalness,
        null as liveness,
        null as valence,
        null as tempo,
        null as track_genre,
        'new_release' as source_type
    from {{ ref('spotify_new_releases_staging') }}
),

trending as (
    select
        track_id,
        track_name,
        artists,
        album_name,
        popularity,
        duration_ms,
        explicit,
        null as danceability,
        null as energy,
        null as speechiness,
        null as acousticness,
        null as instrumentalness,
        null as liveness,
        null as valence,
        null as tempo,
        null as track_genre,
        'trending' as source_type
    from {{ ref('spotify_trending_tracks_staging') }}
)

select * from historical
union all
select * from new_releases
union all
select * from trending

