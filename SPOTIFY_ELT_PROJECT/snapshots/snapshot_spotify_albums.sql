{% snapshot snapshot_spotify_albums %}

{{
    config(
      target_schema='SNAPSHOTS',
      unique_key='album_id',
      strategy='check',
      check_cols=['album_name', 'artist_id', 'release_date', 'album_type', 'num_tracks']
    )
}}

select * from {{ ref('spotify_top_albums_staging') }}

{% endsnapshot %}
