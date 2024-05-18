import { $, Database, Query, Relation } from 'minato'
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
  postId: number
  tagId: number
  post?: Post
  tag?: Tag
}

interface Tables {
  user: User
  profile: Profile
  post: Post
  tag: Tag
  post2tag: Post2Tag
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
    postId: 'unsigned',
    tagId: 'unsigned',
    post: {
      type: 'manyToOne',
      table: 'post',
      target: '_tags',
      fields: 'postId',
    },
    tag: {
      type: 'manyToOne',
      table: 'tag',
      target: '_posts',
      fields: 'tagId',
    },
  }, {
    primary: ['postId', 'tagId'],
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
    { postId: 1, tagId: 1 },
    { postId: 1, tagId: 2 },
    { postId: 2, tagId: 1 },
    { postId: 2, tagId: 3 },
    { postId: 3, tagId: 3 },
  ]

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

      await expect(database.select('user', {}, { profile: true, posts: true }).execute()).to.eventually.have.shape(
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

      await expect(database.select('user', {}, { posts: { author: true } }).execute()).to.eventually.have.shape(
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
      const re = await setup(database, Relation.buildAssociationTable('post', 'tag') as any, post2TagTable.map(x => ({
        post_id: x.postId,
        tag_id: x.tagId,
      })))

      // explicit manyToMany
      await expect(database.select('post', {}, { _tags: { tag: { _posts: { post: true } } } }).execute()).to.eventually.be.fulfilled

      await expect(database.select('post', {}, { tags: { posts: true } }).execute()).to.eventually.have.shape(
        posts.map(post => ({
          ...post,
          tags: post2tags.filter(p2t => p2t.postId === post.id)
            .map(p2t => tags.find(tag => tag.id === p2t.tagId))
            .filter(tag => tag)
            .map(tag => ({
              ...tag,
              posts: post2tags.filter(p2t => p2t.tagId === tag!.id).map(p2t => posts.find(post => post.id === p2t.postId)),
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
        author: {
          id: users.find(user => post.author?.id === user.id)?.id,
        },
      })).filter(post => post.author?.id === 1))

      await expect(database.get('post', {
        author: {
          id: 1,
          value: 0,
        },
      })).to.eventually.have.shape(posts.map(post => ({
        ...post,
        author: users.find(user => post.author?.id === user.id),
      })).filter(post => post.author?.id === 1))
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
      const re = await setup(database, Relation.buildAssociationTable('post', 'tag') as any, post2TagTable.map(x => ({
        post_id: x.postId,
        tag_id: x.tagId,
      })))

      await expect(database.get('post', {
        tags: {
          $some: {
            id: 1,
          },
        },
      })).to.eventually.have.shape(posts.slice(0, 2).map(post => ({
        ...post,
        tags: post2tags.filter(p2t => p2t.postId === post.id)
          .map(p2t => tags.find(tag => tag.id === p2t.tagId))
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
        tags: post2tags.filter(p2t => p2t.postId === post.id)
          .map(p2t => tags.find(tag => tag.id === p2t.tagId))
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
        tags: post2tags.filter(p2t => p2t.postId === post.id)
          .map(p2t => tags.find(tag => tag.id === p2t.tagId))
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
        tags: post2tags.filter(p2t => p2t.postId === post.id)
          .map(p2t => tags.find(tag => tag.id === p2t.tagId))
          .filter(tag => tag),
      })))
    })

    it('nested query', async () => {
      const users = await setup(database, 'user', userTable)
      const profiles = await setup(database, 'profile', profileTable)
      const posts = await setup(database, 'post', postTable)
      const tags = await setup(database, 'tag', tagTable)
      const post2tags = await setup(database, 'post2tag', post2TagTable)
      const re = await setup(database, Relation.buildAssociationTable('post', 'tag') as any, post2TagTable.map(x => ({
        post_id: x.postId,
        tag_id: x.tagId,
      })))

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
        posts: post2tags.filter(p2t => p2t.tagId === tag.id)
          .map(p2t => posts.find(post => post.id === p2t.postId))
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
    })

    it('upsert', async () => {
      await setup(database, 'user', [])
      await setup(database, 'profile', [])
      await setup(database, 'post', [])

      for (const user of userTable) {
        await database.upsert('user', [{
          ...userTable.find(u => u.id === user.id)!,
          profile: profileTable.find(profile => profile.id === user.id),
        }] as any)
      }

      await expect(database.select('user', {}, { profile: true }).execute()).to.eventually.have.shape(
        userTable.map(user => ({
          ...user,
          profile: profileTable.find(profile => profile.id === user.id),
        })),
      )
    })

    it('create oneToMany', async () => {
      await setup(database, 'user', userTable)
      await setup(database, 'profile', profileTable)
      const posts = await setup(database, 'post', postTable)

      posts.push(database.tables['post'].create({ id: posts.length + 1, author: { id: 2 }, content: 'post1' }))
      posts.push(database.tables['post'].create({ id: posts.length + 1, author: { id: 2 }, content: 'post2' }))

      await database.set('user', 2, {
        posts: {
          $create: [
            { content: 'post1' },
            { content: 'post2' },
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
      await database.upsert('user', [{
        id: 1,
        posts: posts.slice(0, 2),
      }])
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

    it('connect / disconnect manyToMany', async () => {
      await setup(database, 'user', userTable)
      await setup(database, 'profile', profileTable)
      await setup(database, 'post', postTable)
      await setup(database, 'tag', tagTable)

      await setup(database, Relation.buildAssociationTable('post', 'tag') as any, post2TagTable.map(x => ({
        post_id: x.postId,
        tag_id: x.tagId,
      })))

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
      await setup(database, Relation.buildAssociationTable('post', 'tag') as any, post2TagTable.map(x => ({
        post_id: x.postId,
        tag_id: x.tagId,
      })))

      posts.filter(post => post2TagTable.some(p2t => p2t.postId === post.id && p2t.tagId === 1)).forEach(post => post.score! += 10)
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
  }

  export function misc(database: Database<Tables>) {
    it('unsupported', async () => {
      await setup(database, 'user', userTable)
      await setup(database, 'post', postTable)
      await setup(database, 'tag', tagTable)

      await expect(database.set('post', 1, {
        tags: [],
      })).to.eventually.be.rejected

      await expect(database.set('post', 1, {
        tags: {
          $remove: {},
        },
      })).to.eventually.be.rejected

      await expect(database.set('post', 1, {
        tags: {
          $set: {},
        },
      })).to.eventually.be.rejected

      await expect(database.set('post', 1, {
        tags: {
          $create: {},
        },
      })).to.eventually.be.rejected
    })
  }
}

export default RelationTests
