import requests
params = {
                'include_profile_interstitial_type': '1',
                'include_blocking': '1',
                'include_blocked_by': '1',
                'include_followed_by': '1',
                'include_want_retweets': '1',
                'include_mute_edge': '1',
                'include_can_dm': '1',
                'include_can_media_tag': '1',
                'skip_status': '1',
                'cards_platform': 'Web-12',
                'include_cards': '1',
                'include_composer_source': 'true',
                'include_ext_alt_text': 'true',
                'include_reply_count': '1',
                'tweet_mode': 'extended',
                'include_entities': 'true',
                'include_user_entities': 'true',
                'include_ext_media_color': 'true',
                'include_ext_media_availability': 'true',
                'send_error_codes': 'true',
                'simple_quoted_tweets': 'true',
                'q': "iraq",
                'tweet_search_mode': 'live',
                'count': '100',
                'query_source': 'spelling_expansion_revert_click',
            }
url = "https://api.twitter.com/2/search/adaptive.json"

payload={}
headers = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.4933 Safari/537.7',
  'Authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs=1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',
  'x-guest-token': '1632702905244344320',
}

response = requests.request("GET", url, headers=headers, data=payload, params=params)

print(response.text)
