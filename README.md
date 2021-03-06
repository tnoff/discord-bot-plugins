# Discord Bot Plugins

Example plugins for discord bot: https://github.com/tnoff/discord-bot.

## Markov

Uses Markov Chain logic: https://en.wikipedia.org/wiki/Markov_chain to mimic chat in discord channels.

Use the command `on` to have the bot read message history for a given channel. Once turned on in a discord channel, reads channel history. For text messages in that channel, it takes text and breaks down each message into "leader" and "follower" pairs. For example, given the text:

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

Print a message to chat with a prompt for users to add emojis (0-9 emojis) to that message, upon which the bot will assign those roles to the users. Note that the bot user must have permissions to give users roles. Bot will assign roles with 0 permissions.

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

## Music

Play music in voice chat from youtube. Songs can be searched via string or a full url can be passed in as well.

Playlists can be created to save songs to be played.

Spotify playlists can be passed into the `!play` function, which will query Spotify API for track metadata and search for those tracks on youtube.

### Commands

```
Music:
  bump         Bump item to top of queue
  clear        Clear all items from queue
  history      Show recently played songs
  join         Connect to voice channel.
  play         Request a song and add it to the queue.
  playlist     Playlist functions.
  queue        Show current song queue
  remove       Remove item from queue.
  shuffle      Shuffle song queue.
  skip         Skip the song.
  stop         Stop the currently playing song and disconnect bot from voice ...

Type !help command for more info on a command.
You can also type !help category for more info on a category.
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

Type !help command for more info on a command.
You can also type !help category for more info on a category.
```