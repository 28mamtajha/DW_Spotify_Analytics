{% snapshot snapshot_spotify_tracks %}

{{
    config(
      target_schema='SNAPSHOTS',
      unique_key='track_id',
      strategy='check',
      check_cols=['popularity', 'duration_ms', 'explicit']
    )
}}

select * from {{ ref('spotify_historical_tracks_staging') }}

{% endsnapshot %}
