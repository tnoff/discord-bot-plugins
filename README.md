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

Allows a bot to set so certain roles can add/remove other roles. Useful if you have a server with a pretty complicated heirarchy structure.

Example Config

```
role:
    <server-id>:
        role_manages:
          reject_list:
            - <role-you-dont-want-ppl-to-edit-id>
          require_roles:
            - <user being added must already have this role>
            - <but only needs one of these roles>
          override_roles:
            - <role-id-which-can-add-any-role-to-any-user>
          <role-id-which-controls-listed-roles>:
            manages:
              - <role-id-which-controller-can-add-or-remove-from>
            only_self: True # Optional arg, user can only add/remove themselves to this role
```

Then you can use commands like
```
!role list # list all roles
!role available # list roles user controls
!role add @user @role # add user to role
!role remove @user @role # remove user from role
```

### Terms

"Required Role" - You must have this role to use the bot for any role
"Managed Role" - A role "managed" by another role, where the role that manages can add/remove users from
"only self" - Users can only add/remove themselves from this role
"Rejected Roles" - Roles not listed at all in any context
"Override Roles" - Role which can add/remove any role to any user w/o checks

### Use Cases

If you want one role to be able to add users to another role, the most basic use case. In the case where you have membership in a channel based on a role, and you want people to add each other easily.

Use `role_controls`, add the ID of the controller role as a key, then under `controls` add a list of roles you want them to "control", meaning add/remove users.

If you want one role to be able to add/remove themselves to and from another role, use the `only_self` flag and set to true. This is for a set of roles you want people to be able to add and remove themselves them from in a self service fashion. This is similar to a role assignment message bot.

### Commands

```
!assign-roles 

Generate message with all roles.
Users can reply to this message to add roles to themselves.
```

## Urban

Lookup a word in urban dictionary and get a definition.

```
!urban

Lookup word on urban dictionary

search: str [Required]
    The word or phrase to search in urban dictionary

Arguments:
  word No description given
```

