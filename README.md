# Discord Bot Plugins

Example plugins for [my Discord Bot implementation](https://github.com/tnoff/discord-bot). These all create custom cogs that inherit from the common `CogHeler` class which can use to inherit settings and a database connection (among other things).


## Music

Play audio from youtube videos in voice chat. The input can be:
- A string that will be searched in youtube
- A direct youtube link

If spotify api credentials are set in the settings, a spotify album url or playlist url can be given as input, which will be used to search for the songs on youtube.
If youtube api credentials are given, a a youtube playlist url can be given, which will add all songs in the youtube playlist.

Songs will be placed into a queue to be downloaded, then added to a play queue for the server to be played in order. The queue can be modified to skip songs, remove songs, bump songs to the top of the queue, and shuffle the queue. A history of the songs played will also be kept until the bot is closed.

Playlists can be created to store video ids to be played. Playlists can be created manually, created from the history of songs played, or created from songs that are currently in the queue.

## Optional Features

Playlists will not be created unless a database engine is provided in the config.

Spotify api options will only be used if spotify api credentials are provided.

Youtube api options will only be used if youtube api credentials are provided.

There is an optional feature to enable "audio processing" on downloaded videos, which will normalize the audio to a common decibal level, and remove empty audio from the beginning and end of a downloaded file.

There is an optional feature to enable file caching, which stores files locally and keeps them around after the bot is closed. This allows files to be re-used later instead of being re-downloaded.

### Commands

```
Music:
  bump         Bump item to top of queue
  clear        Clear all items from queue
  history      Show recently played songs
  join         Connect to voice channel.
  play         Request a song and add it to the queue.
  playlist     Playlist functions.
  random-play  Play random songs from history
  remove       Remove item from queue.
  shuffle      Shuffle song queue.
  skip         Skip the song.
  stop         Stop the currently playing song and disconnect bot from voice...
```

```
!playlist

Playlist functions.

Commands:
  cleanup      Remove items in playlist where we cannot find source
  create       Create new playlist.
  delete       Delete playlist
  item-add     Add item to playlist.
  item-remove  Add item to playlist
  item-search  Find item indexes in playlist that match search
  list         List playlists.
  merge        Merge second playlist into first playlist, deletes second play...
  queue        Add playlist to queue
  rename       Rename playlist to new name
  save-history Save contents of history to a new playlist
  save-queue   Save contents of queue to a new playlist
  show         Show Items in playlist
```

### Config Section

General args that can be used.

Max Song Length: maximum length of a song that can be added to queue, in seconds
Download Dir: Directory where audio files with be downloaded
Queue Max Size: Maximum length of the queue
Enable Audio Processing: Run normalize audio function via moviepy
Server Playlist Max: Max length of created playlists
Enable cache files: Enable local cache files
Max cache files: Maximum number of local cache files to keep before removing

```
music:
  queue_max_size: 256
  max_song_length: 3600
  server_playlist_max: 64
  download_dir: /tmp/discord
  enable_audio_processing: true
  enable_cache_files: true
  max_cache_files: 1024
```

Optional args that can enable spotify and/or youtube playlists.

```
music:
  spotify_client_id: secret-spotify-client
  spotify_client_secret: secret-spotify-client-secret
  youtube_api_key: secret-youtube-api-key
```

## Markov

Uses [Markov Chain](https://en.wikipedia.org/wiki/Markov_chain) to mimic chat in discord channels.

Use the command `on` to have the bot read message history for a given channel. Once turned on in a discord channel, reads channel history to get its data set. There is a loop which will automatically gather new text data, and there is a setting for how far back in time we will keep chat history. For text messages in that channel, it takes text and breaks down each message into "leader" and "follower" pairs. For example, given the text:

```
Hey you guys should check out this cool song
```

It will generate the following pairs:
- "hey" (leader) and "you" (follower)
- "you" (leader) and "guys" (follower)
- "guys" (leader) and "should" (follower)
- etc ...

Then can use the `speak` command to generate a random sentence from channel history.

To mimic user messages, a word can be chosen at random or entered into the command. Given a that word, it finds which pairs have that word as a leader, and which words follow that word. It then calculates the chances a follower comes after a leader word.
For example, given the leader word "hey", there might be a 10% chance the next word is "there", a 25% chance the next word is "everybody", and so on.
In then uses weighted random chance to pick the next word, then uses this word as the leader, and repeats the process for either 32 words by default, or a larger amount of words if specified in the command.

### Commands

```
!markov 

Markov functions

Commands:
  off   Turn markov off for channel
  on    Turn markov on for channel
  speak Say a random sentence generated by markov

Type !help command for more info on a command.
You can also type !help category for more info on a category.
```

## Role

Role assignment bot.

Print a message to chat with a prompt for users to add emojis (0-9 emojis) to that message, upon which the bot will assign those roles to the users. Note that the bot user must have permissions to give users roles. Bot will only assign roles with 0 permissions.

### Commands

```
!assign-roles 

Generate message with all roles.
Users can reply to this message to add roles to themselves.
```

## Twitter

Twitter message bot.

Monitor twitter feeds and post new messages from accounts to specified channels.

### Commands

```
!twitter 

Planner functions

Commands:
  add-filter         Add filter to account subscription
  list-filters       List filter on account subscription
  list-subscriptions List channel subscriptions
  remove-filter      Remove filter from account subscription
  subscribe          Subscribe to twitter account, and post updates in channel
  unsubscribe        Unsubscribe channel from twitter account

Type !help command for more info on a command.
You can also type !help category for more info on a category.
```

### Config Section

You'll need the proper twitter api keys for the bot to use this functionality.

```
twitter:
  consumer_key: secret-consumer-key
  consumer_secret: secret-consumer-secret
  access_token_key: secret-access-token-key
  access_token_secret: secret-access-token-secret
```