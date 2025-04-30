select
    artists,
    count(*) as total_tracks,
    avg(popularity) as avg_popularity,
    avg(duration_ms) as avg_duration,
    avg(danceability) as avg_danceability,
    avg(energy) as avg_energy,
    avg(valence) as avg_valence
from {{ ref('spotify_tracks_all') }}
group by artists
