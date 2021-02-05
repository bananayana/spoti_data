import json
from typing import Dict, Optional, Union

import pandas as pd
import requests
import spotipy.oauth2 as oauth2
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def get_headers(token: str) -> Dict[str, str]:
    """
    Returns basic headers with auth token.
    :param token: auth token from Spotify.
    :return: basic headers
    """
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer ' + token,
    }

    return headers


def get_id(q: str, token: str) -> Optional[str]:
    """
    Tries to find track using a text string `q` (which is expected to contain artist name and track title) and return
    its id. None is returned if no tracks found for a given query (for example track which was in streaming history is
    now unavailable in user's locale) or if a request is timed out.
    :param q: search query
    :param token: auth token
    :return: track id or None
    """
    headers = get_headers(token)
    params = {
        'q': q,
        'type': 'track',
    }
    try:
        response = requests.get(
            'https://api.spotify.com/v1/search',
            headers=headers,
            params=params,
            timeout=5
        )
        response.raise_for_status()
        json = response.json()
        if not len(json['tracks']['items']):
            print(q)
            return None
        first_result = json['tracks']['items'][0]
        track_id = first_result['id']
        return track_id
    except requests.exceptions.Timeout:
        return None


def get_track_data(track_id: str, token: str) -> Dict[str, Union[str, int, float]]:
    """
    Requests track's info for a given track id `id_`.
    :param track_id: track id
    :param token: auth token
    :return: track info
    """
    track_info_url = f'https://api.spotify.com/v1/tracks/{track_id}'
    headers = get_headers(token)
    response = requests.get(track_info_url, headers=headers)
    response.raise_for_status()
    track_data = response.json()
    selected_data = {
        'album_type': track_data['album']['type'],
        'album_id': track_data['album']['id'],
        'album_name': track_data['album']['name'],
        'album_release_date': track_data['album']['release_date'],
        'artist1_id': track_data['artists'][0]['id'],
        'artist1_name': track_data['artists'][0]['name'],
        'artist1_type': track_data['artists'][0]['type'],
        'artist2_id': None if len(track_data['artists']) < 2 else track_data['artists'][1]['id'],
        'artist2_name': None if len(track_data['artists']) < 2 else track_data['artists'][1]['name'],
        'artist2_type': None if len(track_data['artists']) < 2 else track_data['artists'][1]['type'],
        'duration_ms': track_data['duration_ms'],
        'is_local': track_data['is_local'],
        'name': track_data['name'],
        'popularity': track_data['popularity'],
        'type': track_data['type']
    }
    return selected_data


def get_track_audio_features(track_id: str, token: str) -> Optional[Dict[str, Union[int, float, str]]]:
    """
    Requests audio features for a given track id. If audio features are unavailable for this track then None
    is returned.
    :param track_id: track id
    :param token: auth token
    :return: audio features or None
    """
    track_features_url = f'https://api.spotify.com/v1/audio-features/{track_id}'
    headers = get_headers(token)
    response = requests.get(
        track_features_url,
        headers=headers,
        timeout=5
    )
    # for some tracks audio-features are unavailable
    if response.status_code == 503:
        return None
    response.raise_for_status()
    features = response.json()
    return features


def main(streaming_history_json: str, creds_json: str):

    with open(streaming_history_json) as f:
        df = pd.DataFrame(json.load(f))

    df['endTime'] = df.endTime.apply(pd.Timestamp) + pd.offsets.DateOffset(hours=3)

    with open(creds_json) as f:
        data = json.load(f)
        username = data['username']
        client_id = data['client_id']
        client_secret = data['secret']

    # should be the same as in app
    redirect_uri = 'http://localhost:7777/callback'
    scope = 'user-read-recently-played'

    auth_manager = oauth2.SpotifyOAuth(
        username=username,
        scope=scope,
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri
    )

    token = auth_manager.get_access_token()

    df['artist+track'] = df.apply(lambda row: row.artistName + ' ' + row.trackName, axis=1)

    unique_tracks = df['artist+track'].unique()
    id_map = {artist_track: get_id(artist_track, token) for artist_track in unique_tracks}
    'Location restricted tracks'

    df['id_'] = df.apply(lambda row: id_map[row['artist+track']], axis=1)

    tracks_data = []
    for id_ in df['id_'].unique():
        if id_ is None:
            continue
        data = get_track_data(id_, token)
        data['id_'] = id_
        tracks_data.append(data)

    tracks_df = pd.DataFrame(tracks_data)
    tracks_df.to_csv('data/tracks.csv', index=False)

    tracks_features_data = []
    for id_ in tracks_df.id_.unique():
        if id_ is None:
            continue
        data = get_track_audio_features(id_, token)
        if data is None:
            continue
        data['id_'] = id_
        tracks_features_data.append(data)

    keep_features = [
        'id_', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness',
        'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo',
        'time_signature'
    ]

    features_df = pd.DataFrame(tracks_features_data)[keep_features]

    features_df.to_csv('data/features.csv', index=False)
    df.to_csv('data/streaming_history.csv', index=False)
