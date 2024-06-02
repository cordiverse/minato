import { $, Database, Query, Relation, Values } from 'minato'
import { expect } from 'chai'
import { setup } from './utils'

interface User {
  id: number
  value?: number
  profile?: Profile
  posts?: Post[]
  successor?: { id: number }
  predecessor?: { id: number }
}

interface Profile {
  id: number
  name?: string
  user?: User
}

interface Post {
  id: number
  score?: number
  author?: User
  content?: string

  tags?: Tag[]
  _tags?: Post2Tag[]
}

interface Tag {
  id: number
  name: string
  posts?: Post[]
  _posts?: Post2Tag[]
}

interface Post2Tag {
  post?: Post
  tag?: Tag
}

interface GuildSyncRef {
  syncAt: number
}

interface Login extends GuildSyncRef {
  id: string
  platform: string
  name?: string
  guilds?: Guild[]
  syncs?: GuildSync[]
}

interface Guild extends GuildSyncRef {
  id: string
  platform2: string
  name?: string
  logins?: Login[]
  syncs?: GuildSync[]
}

// interface GuildSync extends Login { }
// interface GuildSync extends Guild { }

interface GuildSync {
  syncAt?: number
  guild?: Guild
  login?: Login
}

interface Tables {
  user: User
  profile: Profile
  post: Post
  tag: Tag
  post2tag: Post2Tag
  guildSync: GuildSync
  login: Login
  guild: Guild
}

function RelationTests(database: Database<Tables>) {
  database.extend('profile', {
    id: 'unsigned',
    name: 'string',
  })

  database.extend('user', {
    id: 'unsigned',
    value: 'integer',
    successor: {
      type: 'oneToOne',
      table: 'user',
      target: 'predecessor',
    },
    profile: {
      type: 'oneToOne',
      table: 'profile',
      target: 'user',
    },
  }, {
    autoInc: true,
  })

  database.extend('post', {
    id: 'unsigned',
    score: 'unsigned',
    content: 'string',
    author: {
      type: 'manyToOne',
      table: 'user',
      target: 'posts',
    },
  }, {
    autoInc: true,
  })

  database.extend('tag', {
    id: 'unsigned',
    name: 'string',
    posts: {
      type: 'manyToMany',
      table: 'post',
      target: 'tags',
    },
  }, {
    autoInc: true,
  })

  database.extend('post2tag', {
    id: 'unsigned',
    'post.id': 'unsigned',
    'tag.id': 'unsigned',
    post: {
      type: 'manyToOne',
      table: 'post',
      target: '_tags',
    },
    tag: {
      type: 'manyToOne',
      table: 'tag',
      target: '_posts',
    },
  }, {
    primary: ['post', 'tag'],
  })

  database.extend('login', {
    id: 'string',
    platform: 'string',
    name: 'string',
  }, {
    primary: ['id', 'platform'],
  })

  database.extend('guild', {
    id: 'string',
    platform2: 'string',
    name: 'string',
    logins: {
      type: 'manyToMany',
      table: 'login',
      target: 'guilds',
      shared: { platform2: 'platform' },
    },
  }, {
    primary: ['id', 'platform2'],
  })

  database.extend('guildSync', {
    syncAt: 'unsigned',
    platform: 'string',
    'guild.id': 'string',
    'login.id': 'string',
    guild: {
      type: 'manyToOne',
      table: 'guild',
      target: 'syncs',
      fields: ['guild.id', 'platform'],
    },
    login: {
      type: 'manyToOne',
      table: 'login',
      target: 'syncs',
      fields: ['login.id', 'platform'],
    },
  }, {
    primary: ['platform', 'guild.id', 'login.id'],
  })

  async function setupAutoInc<S, K extends keyof S & string>(database: Database<S>, name: K, length: number) {
    await database.upsert(name, Array(length).fill({}))
    await database.remove(name, {})
  }

  before(async () => {
    await setupAutoInc(database, 'user', 3)
    await setupAutoInc(database, 'post', 3)
    await setupAutoInc(database, 'tag', 3)
  })
}

namespace RelationTests {
  const userTable: User[] = [
    { id: 1, value: 0 },
    { id: 2, value: 1, successor: { id: 1 } },
    { id: 3, value: 2 },
  ]

  const profileTable: Profile[] = [
    { id: 1, name: 'Apple' },
    { id: 2, name: 'Banana' },
    { id: 3, name: 'Cat' },
  ]

  const postTable: Post[] = [
    { id: 1, content: 'A1', author: { id: 1 } },
    { id: 2, content: 'B2', author: { id: 1 } },
    { id: 3, content: 'C3', author: { id: 2 } },
  ]

  const tagTable: Tag[] = [
    { id: 1, name: 'X' },
    { id: 2, name: 'Y' },
    { id: 3, name: 'Z' },
  ]

  const post2TagTable: Post2Tag[] = [
    { post: { id: 1 }, tag: { id: 1 } },
    { post: { id: 1 }, tag: { id: 2 } },
    { post: { id: 2 }, tag: { id: 1 } },
    { post: { id: 2 }, tag: { id: 3 } },
    { post: { id: 3 }, tag: { id: 3 } },
  ] as any

  export interface RelationOptions {
    ignoreNullObject?: boolean
  }

  export function select(database: Database<Tables>, options: RelationOptions = {}) {
    const { ignoreNullObject = true } = options

    it('basic support', async () => {
      const users = await setup(database, 'user', userTable)
      const profiles = await setup(database, 'profile', profileTable)
      const posts = await setup(database, 'post', postTable)

      await expect(database.get('profile', {}, ['user'])).to.eventually.have.shape(
        profiles.map(profile => ({
          user: users.find(user => user.id === profile.id),
        })),
      )

      await expect(database.get('user', {}, { include: { profile: true, posts: true } })).to.eventually.have.shape(
        users.map(user => ({
          ...user,
          profile: profiles.find(profile => profile.id === user.id),
          posts: posts.filter(post => post.author?.id === user.id),
        })),
      )

      await expect(database.select('post', {}, { author: true }).execute()).to.eventually.have.shape(
        posts.map(post => ({
          ...post,
          author: users.find(user => user.id === post.author?.id),
        })),
      )
    })

    ignoreNullObject && it('self relation', async () => {
      const users = await setup(database, 'user', userTable)

      await expect(database.select('user', {}, { successor: true }).execute()).to.eventually.have.shape(
        users.map(user => ({
          ...user,
          successor: users.find(successor => successor.id === user.successor?.id),
        })),
      )
    })

    it('nested reads', async () => {
      const users = await setup(database, 'user', userTable)
      const profiles = await setup(database, 'profile', profileTable)
      const posts = await setup(database, 'post', postTable)

      await expect(database.select('user', {}, { posts: { author: { successor: false } } }).execute()).to.eventually.have.shape(
        users.map(user => ({
          ...user,
          posts: posts.filter(post => post.author?.id === user.id).map(post => ({
            ...post,
            author: users.find(user => user.id === post.author?.id),
          })),
        })),
      )

      await expect(database.select('profile', {}, { user: { posts: { author: true } } }).execute()).to.eventually.have.shape(
        profiles.map(profile => ({
          ...profile,
          user: {
            ...(users.find(user => user.id === profile.id)),
            posts: posts.filter(post => post.author?.id === profile.id).map(post => ({
              ...post,
              author: users.find(user => user.id === profile.id),
            })),
          },
        })),
      )

      await expect(database.select('post', {}, { author: { profile: true } }).execute()).to.eventually.have.shape(
        posts.map(post => ({
          ...post,
          author: {
            ...users.find(user => user.id === post.author?.id),
            profile: profiles.find(profile => profile.id === post.author?.id),
          },
        })),
      )
    })

    it('manyToMany', async () => {
      const users = await setup(database, 'user', userTable)
      const profiles = await setup(database, 'profile', profileTable)
      const posts = await setup(database, 'post', postTable)
      const tags = await setup(database, 'tag', tagTable)
      const post2tags = await setup(database, 'post2tag', post2TagTable)
      const re = await setup(database, Relation.buildAssociationTable('post', 'tag') as any, post2TagTable)

      // explicit manyToMany
      await expect(database.select('post', {}, { _tags: { tag: { _posts: { post: true } } } }).execute()).to.eventually.be.fulfilled

      await expect(database.select('post', {}, { tags: { posts: true } }).execute()).to.eventually.have.shape(
        posts.map(post => ({
          ...post,
          tags: post2tags.filter(p2t => p2t.post?.id === post.id)
            .map(p2t => tags.find(tag => tag.id === p2t.tag?.id))
            .filter(tag => tag)
            .map(tag => ({
              ...tag,
              posts: post2tags.filter(p2t => p2t.tag?.id === tag!.id).map(p2t => posts.find(post => post.id === p2t.post?.id)),
            })),
        })),
      )
    })
  }

  export function query(database: Database<Tables>) {
    it('oneToOne / manyToOne', async () => {
      const users = await setup(database, 'user', userTable)
      const profiles = await setup(database, 'profile', profileTable)
      const posts = await setup(database, 'post', postTable)

      await expect(database.get('user', {
        profile: {
          user: {
            id: 1,
          },
        },
      })).to.eventually.have.shape(users.slice(0, 1).map(user => ({
        ...user,
        profile: profiles.find(profile => profile.id === user.id),
      })))

      await expect(database.get('user', row => $.query(row, {
        profile: r => $.eq(r.id, row.id),
      }))).to.eventually.have.shape(users.map(user => ({
        ...user,
        profile: profiles.find(profile => profile.id === user.id),
      })))

      await expect(database.get('user', {
        profile: {
          user: {
            value: 1,
          },
        },
      })).to.eventually.have.shape(users.slice(1, 2).map(user => ({
        ...user,
        profile: profiles.find(profile => profile.id === user.id),
      })))

      await expect(database.get('post', {
        author: {
          id: 1,
        },
      })).to.eventually.have.shape(posts.map(post => ({
        ...post,
        author: users.find(user => post.author?.id === user.id),
      })).filter(post => post.author?.id === 1))

      await expect(database.get('post', {
        author: {
          id: 1,
          value: 1,
        },
      })).to.eventually.have.length(0)
    })

    it('oneToMany', async () => {
      const users = await setup(database, 'user', userTable)
      const profiles = await setup(database, 'profile', profileTable)
      const posts = await setup(database, 'post', postTable)

      await expect(database.get('user', {
        posts: {
          $some: {
            author: {
              id: 1,
            },
          },
        },
      })).to.eventually.have.shape(users.slice(0, 1).map(user => ({
        ...user,
        posts: posts.filter(post => post.author?.id === user.id),
      })))

      await expect(database.get('user', {
        posts: {
          $some: row => $.eq(row.id, 1),
        },
      })).to.eventually.have.shape(users.slice(0, 1).map(user => ({
        ...user,
        posts: posts.filter(post => post.author?.id === user.id),
      })))

      await expect(database.get('user', {
        posts: {
          $none: {
            author: {
              id: 1,
            },
          },
        },
      })).to.eventually.have.shape(users.slice(1).map(user => ({
        ...user,
        posts: posts.filter(post => post.author?.id === user.id),
      })))

      await expect(database.get('user', {
        posts: {
          $every: {
            author: {
              id: 1,
            },
          },
        },
      })).to.eventually.have.shape([users[0], users[2]].map(user => ({
        ...user,
        posts: posts.filter(post => post.author?.id === user.id),
      })))
    })

    it('manyToMany', async () => {
      const users = await setup(database, 'user', userTable)
      const profiles = await setup(database, 'profile', profileTable)
      const posts = await setup(database, 'post', postTable)
      const tags = await setup(database, 'tag', tagTable)
      const post2tags = await setup(database, 'post2tag', post2TagTable)
      const re = await setup(database, Relation.buildAssociationTable('post', 'tag') as any, post2TagTable)

      await expect(database.get('post', {
        tags: {
          $some: {
            id: 1,
          },
        },
      })).to.eventually.have.shape(posts.slice(0, 2).map(post => ({
        ...post,
        tags: post2tags.filter(p2t => p2t.post?.id === post.id)
          .map(p2t => tags.find(tag => tag.id === p2t.tag?.id))
          .filter(tag => tag),
      })))

      await expect(database.get('post', {
        tags: {
          $none: {
            id: 1,
          },
        },
      })).to.eventually.have.shape(posts.slice(2).map(post => ({
        ...post,
        tags: post2tags.filter(p2t => p2t.post?.id === post.id)
          .map(p2t => tags.find(tag => tag.id === p2t.tag?.id))
          .filter(tag => tag),
      })))

      await expect(database.get('post', {
        tags: {
          $every: {
            id: 3,
          },
        },
      })).to.eventually.have.shape(posts.slice(2, 3).map(post => ({
        ...post,
        tags: post2tags.filter(p2t => p2t.post?.id === post.id)
          .map(p2t => tags.find(tag => tag.id === p2t.tag?.id))
          .filter(tag => tag),
      })))

      await expect(database.get('post', {
        tags: {
          $some: 1,
          $none: [3],
          $every: {},
        },
      })).to.eventually.have.shape(posts.slice(0, 1).map(post => ({
        ...post,
        tags: post2tags.filter(p2t => p2t.post?.id === post.id)
          .map(p2t => tags.find(tag => tag.id === p2t.tag?.id))
          .filter(tag => tag),
      })))
    })

    it('nested query', async () => {
      const users = await setup(database, 'user', userTable)
      const profiles = await setup(database, 'profile', profileTable)
      const posts = await setup(database, 'post', postTable)
      const tags = await setup(database, 'tag', tagTable)
      const post2tags = await setup(database, 'post2tag', post2TagTable)
      const re = await setup(database, Relation.buildAssociationTable('post', 'tag') as any, post2TagTable)

      await expect(database.get('user', {
        posts: {
          $some: {
            tags: {
              $some: {
                id: 1,
              },
            },
          },
        },
      })).to.eventually.have.shape([users[0]].map(user => ({
        ...user,
        posts: posts.filter(post => post.author?.id === user.id),
      })))

      await expect(database.get('tag', {
        posts: {
          $some: {
            author: {
              id: 2,
            },
          },
        },
      })).to.eventually.have.shape([tags[2]].map(tag => ({
        ...tag,
        posts: post2tags.filter(p2t => p2t.tag?.id === tag.id)
          .map(p2t => posts.find(post => post.id === p2t.post?.id))
          .filter(post => post),
      })))
    })

    it('omit query', async () => {
      const users = await setup(database, 'user', userTable)
      const profiles = await setup(database, 'profile', profileTable)

      await expect(database.get('user', { id: 2 }, ['id', 'profile'])).to.eventually.have.shape(
        [users[1]].map(user => ({
          id: user.id,
          profile: profiles.find(profile => user.id === profile.id),
        })),
      )
    })

    it('existence', async () => {
      await setup(database, 'user', userTable)
      await setup(database, 'profile', profileTable)
      await setup(database, 'post', postTable)

      await expect(database.select('user', { successor: null }, null).execute()).to.eventually.have.shape([
        { id: 1 },
        { id: 3 },
      ])

      await expect(database.select('user', { predecessor: null }, null).execute()).to.eventually.have.shape([
        { id: 2 },
        { id: 3 },
      ])

      await database.set('user', 1, { profile: null })
      await expect(database.select('user', { profile: null }, null).execute()).to.eventually.have.shape([
        { id: 1 },
      ])

      await database.set('user', 2, {
        posts: {
          $disconnect: {
            id: 3,
          },
        },
      })
      await expect(database.select('post', { author: null }, null).execute()).to.eventually.have.shape([
        { id: 3 },
      ])
      await expect(database.select('user', {
        posts: {
          $every: {
            author: null,
          },
        },
      }, null).execute()).to.eventually.have.shape([
        { id: 2 },
        { id: 3 },
      ])
    })

    it('manyToOne fallbacck', async () => {
      await setup(database, 'user', [])
      await setup(database, 'profile', [])
      await setup(database, 'post', [])

      await database.create('post', {
        id: 1,
        content: 'new post',
        author: {
          id: 2,
        },
      })

      await expect(database.get('post', 1, ['author'])).to.eventually.have.shape([{
        author: {
          id: 2,
        },
      }])

      await database.create('user', {
        id: 2,
        value: 123,
      })

      await expect(database.get('post', 1, ['author'])).to.eventually.have.shape([{
        author: {
          id: 2,
          value: 123,
        },
      }])
    })
  }

  export function create(database: Database<Tables>) {
    it('basic support', async () => {
      await setup(database, 'user', [])
      await setup(database, 'profile', [])
      await setup(database, 'post', [])

      for (const user of userTable) {
        await expect(database.create('user', {
          ...user,
          profile: {
            ...profileTable.find(profile => profile.id === user.id)!,
          },
          posts: postTable.filter(post => post.author?.id === user.id),
        })).to.eventually.have.shape(user)
      }

      await expect(database.select('profile', {}, { user: true }).execute()).to.eventually.have.shape(
        profileTable.map(profile => ({
          ...profile,
          user: userTable.find(user => user.id === profile.id),
        })),
      )

      await expect(database.select('user', {}, { profile: true, posts: true }).execute()).to.eventually.have.shape(
        userTable.map(user => ({
          ...user,
          profile: profileTable.find(profile => profile.id === user.id),
          posts: postTable.filter(post => post.author?.id === user.id),
        })),
      )
    })

    it('oneToMany', async () => {
      await setup(database, 'user', [])
      await setup(database, 'profile', [])
      await setup(database, 'post', [])

      for (const user of userTable) {
        await database.create('user', {
          ...userTable.find(u => u.id === user.id)!,
          posts: postTable.filter(post => post.author?.id === user.id),
        })
      }

      await expect(database.select('user', {}, { posts: true }).execute()).to.eventually.have.shape(
        userTable.map(user => ({
          ...user,
          posts: postTable.filter(post => post.author?.id === user.id),
        })),
      )
    })

    it('upsert / connect oneToMany / manyToOne', async () => {
      await setup(database, 'user', [])
      await setup(database, 'profile', [])
      await setup(database, 'post', [])

      await database.create('user', {
        id: 1,
        value: 1,
        posts: {
          $upsert: [
            {
              id: 1,
              content: 'post1',
            },
            {
              id: 2,
              content: 'post2',
            },
          ],
        },
      })

      await expect(database.select('user', 1, { posts: true }).execute()).to.eventually.have.shape([
        {
          id: 1,
          value: 1,
          posts: [
            { id: 1, content: 'post1' },
            { id: 2, content: 'post2' },
          ],
        },
      ])

      await database.create('user', {
        id: 2,
        value: 2,
        posts: {
          $create: [
            {
              id: 3,
              content: 'post3',
              author: {
                $upsert: {
                  id: 2,
                  value: 3,
                },
              },
            },
            {
              id: 4,
              content: 'post4',
              author: {
                $connect: {
                  id: 1,
                },
              },
            },
          ],
        },
      })

      await expect(database.select('user', {}, { posts: true }).execute()).to.eventually.have.shape([
        {
          id: 1,
          value: 1,
          posts: [
            { id: 1, content: 'post1' },
            { id: 2, content: 'post2' },
            { id: 4, content: 'post4' },
          ],
        },
        {
          id: 2,
          value: 3,
          posts: [
            { id: 3, content: 'post3' },
          ],
        },
      ])
    })

    it('manyToOne', async () => {
      const users = await setup(database, 'user', [])
      await setup(database, 'post', [])

      users.push({ id: 1, value: 2 })

      await database.create('post', {
        id: 1,
        content: 'post2',
        author: {
          $create: {
            id: 1,
            value: 2,
          },
        },
      })
      await expect(database.get('user', {})).to.eventually.have.shape(users)

      users[0].value = 3
      await database.create('post', {
        id: 2,
        content: 'post3',
        author: {
          $create: {
            id: 1,
            value: 3,
          },
        },
      })
      await expect(database.get('user', {})).to.eventually.have.shape(users)

      await database.create('post', {
        id: 3,
        content: 'post4',
        author: {
          id: 1,
        },
      })
      await expect(database.get('user', {})).to.eventually.have.shape(users)
      await expect(database.get('post', {}, { include: { author: true } })).to.eventually.have.shape([
        { id: 1, content: 'post2', author: { id: 1, value: 3 } },
        { id: 2, content: 'post3', author: { id: 1, value: 3 } },
        { id: 3, content: 'post4', author: { id: 1, value: 3 } },
      ])
    })

    it('manyToMany', async () => {
      await setup(database, 'user', [])
      await setup(database, 'post', [])
      await setup(database, 'tag', [])
      await setup(database, Relation.buildAssociationTable('post', 'tag') as any, [])

      for (const user of userTable) {
        await database.create('user', {
          ...userTable.find(u => u.id === user.id)!,
          posts: postTable.filter(post => post.author?.id === user.id).map(post => ({
            ...post,
            tags: {
              $upsert: post2TagTable.filter(p2t => p2t.post?.id === post.id).map(p2t => tagTable.find(tag => tag.id === p2t.tag?.id)).filter(x => !!x),
            },
          })),
        })
      }

      await expect(database.select('user', {}, { posts: { tags: true } }).execute()).to.eventually.have.shape(
        userTable.map(user => ({
          ...user,
          posts: postTable.filter(post => post.author?.id === user.id).map(post => ({
            ...post,
            tags: post2TagTable.filter(p2t => p2t.post?.id === post.id).map(p2t => tagTable.find(tag => tag.id === p2t.tag?.id)),
          })),
        })),
      )
    })

    it('manyToMany expr', async () => {
      await setup(database, 'user', [])
      await setup(database, 'post', [])
      await setup(database, 'tag', [])
      await setup(database, Relation.buildAssociationTable('post', 'tag') as any, [])

      await database.create('post', {
        id: 1,
        content: 'post1',
        author: {
          $create: {
            id: 1,
            value: 1,
          },
        },
        tags: {
          $create: [
            {
              name: 'tag1',
            },
            {
              name: 'tag2',
            },
          ],
        },
      })

      await database.create('post', {
        id: 2,
        content: 'post2',
        author: {
          $connect: {
            id: 1,
          },
        },
        tags: {
          $connect: {
            name: 'tag1',
          },
        },
      })

      await expect(database.select('user', {}, { posts: { tags: true } }).execute()).to.eventually.have.shape([
        {
          id: 1,
          value: 1,
          posts: [
            {
              id: 1,
              content: 'post1',
              tags: [
                { name: 'tag1' },
                { name: 'tag2' },
              ],
            },
            {
              id: 2,
              content: 'post2',
              tags: [
                { name: 'tag1' },
              ],
            },
          ],
        },
      ])
    })

    it('explicit manyToMany', async () => {
      await setup(database, 'login', [])
      await setup(database, 'guild', [
        { id: '1', platform2: 'sandbox', name: 'Guild1' },
        { id: '2', platform2: 'sandbox', name: 'Guild2' },
        { id: '3', platform2: 'sandbox', name: 'Guild3' },
      ])
      await setup(database, Relation.buildAssociationTable('login', 'guild') as any, [])

      await database.create('login', {
        id: '1',
        platform: 'sandbox',
        name: 'Bot1',
        syncs: {
          $create: [
            {
              syncAt: 123,
              guild: {
                $connect: { id: '1' },
              },
            },
          ],
        },
      })

      await expect(database.get('login', {
        id: '1',
        platform: 'sandbox',
      }, {
        include: { syncs: { guild: true } },
      })).to.eventually.have.nested.property('[0].syncs').with.length(1)
    })
  }

  export function modify(database: Database<Tables>, options: RelationOptions = {}) {
    const { ignoreNullObject = true } = options

    it('oneToOne / manyToOne', async () => {
      const users = await setup(database, 'user', userTable)
      const profiles = await setup(database, 'profile', profileTable)
      await setup(database, 'post', postTable)

      profiles.splice(2, 1)
      await database.set('user', 3, {
        profile: null,
      })
      await expect(database.get('profile', {})).to.eventually.have.deep.members(profiles)

      profiles.push(database.tables['profile'].create({ id: 3, name: 'Reborn' }))
      await database.set('user', 3, {
        profile: {
          name: 'Reborn',
        },
      })
      await expect(database.get('profile', {})).to.eventually.have.deep.members(profiles)

      users[0].value = 99
      await database.set('post', 1, {
        author: {
          value: 99,
        },
      })
      await expect(database.get('user', {})).to.eventually.have.deep.members(users)

      profiles.splice(2, 1)
      await database.set('user', 3, {
        profile: null,
      })
      await expect(database.get('profile', {})).to.eventually.have.deep.members(profiles)

      users.push({ id: 100, value: 200, successor: { id: undefined } } as any)
      await database.set('post', 1, {
        author: {
          id: 100,
          value: 200,
        },
      })
      await expect(database.get('user', {})).to.eventually.have.deep.members(users)
    })

    it('oneToOne expr', async () => {
      await setup(database, 'user', [])
      await setup(database, 'profile', [])
      await setup(database, 'post', [])

      await database.create('user', {
        id: 1,
        value: 0,
      })

      await database.set('user', 1, {
        profile: {
          $create: {
            name: 'Apple',
          },
        },
      })
      await expect(database.select('user', {}, { profile: true }).execute()).to.eventually.have.shape(
        [{ id: 1, value: 0, profile: { name: 'Apple' } }],
      )

      await database.set('user', 1, {
        profile: {
          $upsert: [{
            name: 'Apple2',
          }],
        },
      })
      await expect(database.select('user', {}, { profile: true }).execute()).to.eventually.have.shape(
        [{ id: 1, value: 0, profile: { name: 'Apple2' } }],
      )

      await database.set('user', 1, {
        profile: {
          $set: r => ({
            name: $.concat(r.name, '3'),
          }),
        },
      })
      await expect(database.select('user', {}, { profile: true }).execute()).to.eventually.have.shape(
        [{ id: 1, value: 0, profile: { name: 'Apple23' } }],
      )
    })

    ignoreNullObject && it('manyToOne expr', async () => {
      await setup(database, 'user', [])
      await setup(database, 'profile', [])
      await setup(database, 'post', [])

      await database.create('post', {
        id: 1,
        content: 'Post1',
      })

      await database.set('post', 1, {
        author: {
          $create: {
            id: 1,
            value: 0,
            profile: {
              $create: {
                name: 'Apple',
              },
            },
          },
        },
      })
      await expect(database.select('user', {}, { posts: true, profile: true }).execute()).to.eventually.have.shape(
        [{ id: 1, value: 0, profile: { name: 'Apple' }, posts: [{ id: 1, content: 'Post1' }] }],
      )

      await database.set('post', 1, {
        author: {
          $set: r => ({
            value: 123,
          }),
        },
      })
      await expect(database.select('user', {}, { posts: true, profile: true }).execute()).to.eventually.have.shape(
        [{ id: 1, value: 123, profile: { name: 'Apple' }, posts: [{ id: 1, content: 'Post1' }] }],
      )

      await database.set('post', 1, {
        author: null,
      })
      await expect(database.select('user', {}, { posts: true, profile: true }).execute()).to.eventually.have.shape(
        [{ id: 1, value: 123, profile: { name: 'Apple' }, posts: [] }],
      )
    })

    it('create oneToMany', async () => {
      await setup(database, 'user', userTable)
      await setup(database, 'profile', profileTable)
      const posts = await setup(database, 'post', postTable)

      posts.push(database.tables['post'].create({ id: 4, author: { id: 2 }, content: 'post1' }))
      posts.push(database.tables['post'].create({ id: 5, author: { id: 2 }, content: 'post2' }))
      posts.push(database.tables['post'].create({ id: 6, author: { id: 2 }, content: 'post1' }))
      posts.push(database.tables['post'].create({ id: 7, author: { id: 2 }, content: 'post2' }))

      await database.set('user', 2, {
        posts: {
          $create: [
            { id: 4, content: 'post1' },
            { id: 5, content: 'post2' },
          ],
          $upsert: [
            { id: 6, content: 'post1' },
            { id: 7, content: 'post2' },
          ],
        },
      })
      await expect(database.get('post', {})).to.eventually.have.deep.members(posts)

      posts.push(database.tables['post'].create({ id: 101, author: { id: 1 }, content: 'post101' }))
      await database.set('user', 1, row => ({
        value: $.add(row.id, 98),
        posts: {
          $create: { id: 101, content: 'post101' },
        },
      }))
      await expect(database.get('post', {})).to.eventually.have.deep.members(posts)
    })

    it('set oneToMany', async () => {
      await setup(database, 'user', userTable)
      await setup(database, 'profile', profileTable)
      const posts = await setup(database, 'post', postTable)

      posts[0].score = 2
      posts[1].score = 3
      await database.set('user', 1, row => ({
        posts: {
          $set: r => ({
            score: $.add(row.id, r.id),
          }),
        },
      }))
      await expect(database.get('post', {})).to.eventually.have.deep.members(posts)

      posts[1].score = 13
      await database.set('user', 1, row => ({
        posts: {
          $set: {
            where: { score: { $gt: 2 } },
            update: r => ({ score: $.add(r.score, 10) }),
          },
        },
      }))
      await expect(database.get('post', {})).to.eventually.have.deep.members(posts)
    })

    it('delete oneToMany', async () => {
      await setup(database, 'user', userTable)
      await setup(database, 'profile', profileTable)
      const posts = await setup(database, 'post', postTable)

      posts.splice(0, 1)
      await database.set('user', {}, row => ({
        posts: {
          $remove: r => $.eq(r.id, row.id),
        },
      }))
      await expect(database.get('post', {})).to.eventually.have.deep.members(posts)

      await database.set('post', 2, {
        author: {
          $remove: {},
          $connect: { id: 2 },
        },
      })
      await database.set('post', 3, {
        author: {
          $disconnect: {},
        },
      })
      await expect(database.get('user', {}, { include: { posts: true } })).to.eventually.have.shape([
        {
          id: 2,
          posts: [
            { id: 2 },
          ],
        },
        {
          id: 3,
          posts: [],
        },
      ])
    })

    it('override oneToMany', async () => {
      await setup(database, 'user', userTable)
      await setup(database, 'profile', profileTable)
      const posts = await setup(database, 'post', postTable)

      posts[0].score = 2
      posts[1].score = 3
      await database.set('user', 1, row => ({
        posts: posts.slice(0, 2),
      }))
      await expect(database.get('post', {})).to.eventually.have.deep.members(posts)

      posts[0].score = 4
      posts[1].score = 5
      await database.set('user', 1, {
        posts: posts.slice(0, 2),
      })
      await expect(database.get('post', {})).to.eventually.have.deep.members(posts)
    })

    ignoreNullObject && it('connect / disconnect oneToMany', async () => {
      await setup(database, 'user', userTable)
      await setup(database, 'profile', profileTable)
      await setup(database, 'post', postTable)

      await database.set('user', 1, {
        posts: {
          $disconnect: {},
          $connect: { id: 3 },
        },
      })
      await expect(database.get('user', 1, ['posts'])).to.eventually.have.shape([{
        posts: [
          { id: 3 },
        ],
      }])
    })

    it('modify manyToMany', async () => {
      await setup(database, 'user', userTable)
      await setup(database, 'profile', profileTable)
      await setup(database, 'post', postTable)
      await setup(database, 'tag', [])

      await setup(database, Relation.buildAssociationTable('post', 'tag') as any, [])

      await database.set('post', 2, {
        tags: {
          $create: {
            id: 1,
            name: 'Tag1',
          },
          $upsert: [
            {
              id: 2,
              name: 'Tag2',
            },
            {
              id: 3,
              name: 'Tag3',
            },
          ],
        },
      })
      await expect(database.get('post', 2, ['tags'])).to.eventually.have.nested.property('[0].tags').with.shape([
        { id: 1, name: 'Tag1' },
        { id: 2, name: 'Tag2' },
        { id: 3, name: 'Tag3' },
      ])

      await database.set('post', 2, row => ({
        tags: {
          $set: r => ({
            name: $.concat(r.name, row.content, '2'),
          }),
          $remove: {
            id: 3,
          },
        },
      }))
      await expect(database.get('post', 2, ['tags'])).to.eventually.have.nested.property('[0].tags').with.shape([
        { id: 1, name: 'Tag1B22' },
        { id: 2, name: 'Tag2B22' },
      ])

      await database.set('post', 2, {
        tags: [
          { id: 1, name: 'Tag1' },
          { id: 2, name: 'Tag2' },
          { id: 3, name: 'Tag3' },
        ],
      })
      await expect(database.get('post', 2, ['tags'])).to.eventually.have.nested.property('[0].tags').with.shape([
        { id: 1, name: 'Tag1' },
        { id: 2, name: 'Tag2' },
        { id: 3, name: 'Tag3' },
      ])
    })

    it('connect / disconnect manyToMany', async () => {
      await setup(database, 'user', userTable)
      await setup(database, 'profile', profileTable)
      await setup(database, 'post', postTable)
      await setup(database, 'tag', tagTable)

      await setup(database, Relation.buildAssociationTable('post', 'tag') as any, post2TagTable)

      await database.set('post', 2, {
        tags: {
          $disconnect: {},
        },
      })
      await expect(database.get('post', 2, ['tags'])).to.eventually.have.nested.property('[0].tags').deep.equal([])

      await database.set('post', 2, row => ({
        tags: {
          $connect: r => $.eq(r.id, row.id),
        },
      }))
      await expect(database.get('post', 2, ['tags'])).to.eventually.have.nested.property('[0].tags').with.shape([{
        id: 2,
      }])
    })

    it('query relation', async () => {
      await setup(database, 'user', userTable)
      const posts = await setup(database, 'post', postTable)
      await setup(database, 'tag', tagTable)
      await setup(database, Relation.buildAssociationTable('post', 'tag') as any, post2TagTable)

      posts.filter(post => post2TagTable.some(p2t => p2t.post?.id === post.id && p2t.tag?.id === 1)).forEach(post => post.score! += 10)
      await database.set('post', {
        tags: {
          $some: {
            id: 1,
          },
        },
      }, row => ({
        score: $.add(row.score, 10),
      }))
      await expect(database.get('post', {})).to.eventually.have.deep.members(posts)
    })

    it('shared manyToMany', async () => {
      await setup(database, 'login', [
        { id: '1', platform: 'sandbox', name: 'Bot1' },
        { id: '2', platform: 'sandbox', name: 'Bot2' },
        { id: '3', platform: 'sandbox', name: 'Bot3' },
        { id: '1', platform: 'whitebox', name: 'Bot1' },
      ])
      await setup(database, 'guild', [
        { id: '1', platform2: 'sandbox', name: 'Guild1' },
        { id: '2', platform2: 'sandbox', name: 'Guild2' },
        { id: '3', platform2: 'sandbox', name: 'Guild3' },
        { id: '1', platform2: 'whitebox', name: 'Guild1' },
      ])
      await setup(database, Relation.buildAssociationTable('login', 'guild') as any, [])

      await database.set('login', {
        id: '1',
        platform: 'sandbox',
      }, {
        guilds: {
          $connect: {
            id: {
              $or: ['1', '2'],
            },
          },
        },
      })
      await expect(database.get('login', {
        id: '1',
        platform: 'sandbox',
      }, ['guilds'])).to.eventually.have.nested.property('[0].guilds').with.length(2)

      await database.set('login', {
        id: '1',
        platform: 'sandbox',
      }, {
        guilds: {
          $disconnect: {
            id: '2',
          },
        },
      })

      await expect(database.get('login', {
        id: '1',
        platform: 'sandbox',
      }, ['guilds'])).to.eventually.have.nested.property('[0].guilds').with.length(1)

      await database.create('guild', {
        id: '4',
        platform2: 'sandbox',
        name: 'Guild4',
        logins: {
          $upsert: [
            { id: '1' },
            { id: '2' },
          ],
        },
      })

      await expect(database.get('login', { platform: 'sandbox' }, ['id', 'guilds'])).to.eventually.have.shape([
        { id: '1', guilds: [{ id: '1' }, { id: '4' }] },
        { id: '2', guilds: [{ id: '4' }] },
        { id: '3', guilds: [] },
      ])

      await expect(database.get('guild', { platform2: 'sandbox' }, ['id', 'logins'])).to.eventually.have.shape([
        { id: '1', logins: [{ id: '1' }] },
        { id: '2', logins: [] },
        { id: '3', logins: [] },
        { id: '4', logins: [{ id: '1' }, { id: '2' }] },
      ])
    })
  }
}

export default RelationTests
