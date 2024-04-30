import { $, Database, Relation } from 'minato'
import { expect } from 'chai'
import { setup } from './utils'

interface User {
  id: number
  value?: number
  profile?: Relation<Profile>
  posts?: Relation<Post[]>
}

interface Profile {
  name?: string
  userId: number
  user?: Relation<User>
}

interface Post {
  id: number
  score?: number
  authorId?: number
  author?: Relation<User>
  content?: string

  tags?: Relation<Tag[]>
  _tags?: Relation<Post2Tag[]>
}

interface Tag {
  id: number
  name: string
  posts?: Relation<Post[]>
  _posts?: Relation<Post2Tag[]>
}

interface Post2Tag {
  postId: number
  tagId: number
  post?: Relation<Post>
  tag?: Relation<Tag>
}

interface Tables {
  user: User
  profile: Profile
  post: Post
  tag: Tag
  post2tag: Post2Tag
}

function RelationTests(database: Database<Tables>) {
  database.extend('user', {
    id: 'unsigned',
    value: 'integer',
  }, {
    autoInc: true,
  })

  database.extend('profile', {
    name: 'string',
    userId: 'unsigned',
    user: {
      type: 'oneToOne',
      target: ['user', 'profile'],
      fields: 'userId',
      references: 'id',
    },
  }, {
    primary: ['userId'],
  })

  database.extend('post', {
    id: 'unsigned',
    score: 'unsigned',
    content: 'string',
    authorId: 'unsigned',
    author: {
      type: 'manyToOne',
      target: ['user', 'posts'],
      fields: 'authorId',
      references: 'id',
    },
  }, {
    autoInc: true,
  })

  database.extend('tag', {
    id: 'unsigned',
    name: 'string',
    posts: {
      type: 'manyToMany',
      target: ['post', 'tags'],
      fields: 'id',
      references: 'id',
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
      target: ['post', '_tags'],
      fields: 'postId',
      references: 'id',
    },
    tag: {
      type: 'manyToOne',
      target: ['tag', '_posts'],
      fields: 'tagId',
      references: 'id',
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
    { id: 2, value: 1 },
    { id: 3, value: 2 },
  ]

  const profileTable: Profile[] = [
    { userId: 1, name: 'Apple' },
    { userId: 2, name: 'Banana' },
    { userId: 3, name: 'Cat' },
  ]

  const postTable: Post[] = [
    { id: 1, content: 'A1', authorId: 1 },
    { id: 2, content: 'B2', authorId: 1 },
    { id: 3, content: 'C3', authorId: 2 },
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

  export function select(database: Database<Tables>) {
    it('basic support', async () => {
      const users = await setup(database, 'user', userTable)
      const profiles = await setup(database, 'profile', profileTable)
      const posts = await setup(database, 'post', postTable)

      await expect(database.get('profile', {}, ['user'])).to.eventually.have.shape(
        profiles.map(profile => ({
          user: users.find(user => user.id === profile.userId),
        })),
      )

      await expect(database.get('user', {}, ['id', 'value', 'profile', 'posts'])).to.eventually.have.shape(
        users.map(user => ({
          ...user,
          profile: profiles.find(profile => profile.userId === user.id),
          posts: posts.filter(post => post.authorId === user.id),
        })),
      )

      await expect(database.select('post', {}, { author: true }).execute()).to.eventually.have.shape(
        posts.map(post => ({
          ...post,
          author: users.find(user => user.id === post.authorId),
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
          posts: posts.filter(post => post.authorId === user.id).map(post => ({
            ...post,
            author: users.find(user => user.id === post.authorId),
          })),
        })),
      )

      await expect(database.select('profile', {}, { user: { posts: { author: true } } }).execute()).to.eventually.have.shape(
        profiles.map(profile => ({
          ...profile,
          user: {
            ...(users.find(user => user.id === profile.userId)),
            posts: posts.filter(post => post.authorId === profile.userId).map(post => ({
              ...post,
              author: users.find(user => user.id === profile.userId),
            })),
          },
        })),
      )

      await expect(database.select('post', {}, { author: { profile: true } }).execute()).to.eventually.have.shape(
        posts.map(post => ({
          ...post,
          author: {
            ...users.find(user => user.id === post.authorId),
            profile: profiles.find(profile => profile.userId === post.authorId),
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
          userId: 1,
        },
      })).to.eventually.have.shape(users.slice(0, 1).map(user => ({
        ...user,
        profile: profiles.find(profile => profile.userId === user.id),
      })))

      await expect(database.get('user', row => ({
        $expr: true,
        profile: r => $.eq(r.userId, row.id),
      }))).to.eventually.have.shape(users.map(user => ({
        ...user,
        profile: profiles.find(profile => profile.userId === user.id),
      })))

      await expect(database.get('user', {
        profile: {
          user: {
            value: 1,
          },
        },
      })).to.eventually.have.shape(users.slice(1, 2).map(user => ({
        ...user,
        profile: profiles.find(profile => profile.userId === user.id),
      })))

      await expect(database.get('post', {
        author: {
          id: 1,
        },
      })).to.eventually.have.shape(posts.map(post => ({
        ...post,
        author: users.find(user => post.authorId === user.id),
      })).filter(post => post.author?.id === 1))
    })

    it('oneToMany', async () => {
      const users = await setup(database, 'user', userTable)
      const profiles = await setup(database, 'profile', profileTable)
      const posts = await setup(database, 'post', postTable)

      await expect(database.get('user', {
        posts: {
          $some: {
            authorId: 1,
          },
        },
      })).to.eventually.have.shape(users.slice(0, 1).map(user => ({
        ...user,
        posts: posts.filter(post => post.authorId === user.id),
      })))

      await expect(database.get('user', {
        posts: {
          $some: row => $.eq(row.id, 1),
        },
      })).to.eventually.have.shape(users.slice(0, 1).map(user => ({
        ...user,
        posts: posts.filter(post => post.authorId === user.id),
      })))

      await expect(database.get('user', {
        posts: {
          $none: {
            authorId: 1,
          },
        },
      })).to.eventually.have.shape(users.slice(1).map(user => ({
        ...user,
        posts: posts.filter(post => post.authorId === user.id),
      })))

      await expect(database.get('user', {
        posts: {
          $every: {
            authorId: 1,
          },
        },
      })).to.eventually.have.shape([users[0], users[2]].map(user => ({
        ...user,
        posts: posts.filter(post => post.authorId === user.id),
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
          $some: {
            id: 1,
          },
          $none: {
            id: 3,
          },
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
        posts: posts.filter(post => post.authorId === user.id),
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
  }

  export function create(database: Database<Tables>) {
    it('basic support', async () => {
      await setup(database, 'user', [])
      await setup(database, 'profile', [])
      await setup(database, 'post', [])

      for (const profile of profileTable) {
        await expect(database.create('profile', {
          ...profile,
          user: {
            ...userTable.find(user => profile.userId === user.id)!,
            posts: postTable.filter(post => post.authorId === profile.userId),
          },
        })).to.eventually.have.shape(profile)
      }

      await expect(database.select('profile', {}, { user: true }).execute()).to.eventually.have.shape(
        profileTable.map(profile => ({
          ...profile,
          user: userTable.find(user => user.id === profile.userId),
        })),
      )

      await expect(database.select('user', {}, { profile: true, posts: true }).execute()).to.eventually.have.shape(
        userTable.map(user => ({
          ...user,
          profile: profileTable.find(profile => profile.userId === user.id),
          posts: postTable.filter(post => post.authorId === user.id),
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
          posts: postTable.filter(post => post.authorId === user.id),
        })
      }

      await expect(database.select('user', {}, { posts: true }).execute()).to.eventually.have.shape(
        userTable.map(user => ({
          ...user,
          posts: postTable.filter(post => post.authorId === user.id),
        })),
      )
    })
  }

  export function modify(database: Database<Tables>) {
    it('oneToOne / manyToOne', async () => {
      const users = await setup(database, 'user', userTable)
      const profiles = await setup(database, 'profile', profileTable)
      await setup(database, 'post', postTable)

      profiles.splice(2, 1)
      await database.set('user', 3, {
        profile: null,
      })
      await expect(database.get('profile', {})).to.eventually.have.deep.members(profiles)

      profiles.push(database.tables['profile'].create({ userId: 3, name: 'Reborn' }))
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
          profile: profileTable.find(profile => profile.userId === user.id),
        }] as any)
      }

      await expect(database.select('user', {}, { profile: true }).execute()).to.eventually.have.shape(
        userTable.map(user => ({
          ...user,
          profile: profileTable.find(profile => profile.userId === user.id),
        })),
      )
    })

    it('create oneToMany', async () => {
      await setup(database, 'user', userTable)
      await setup(database, 'profile', profileTable)
      const posts = await setup(database, 'post', postTable)

      posts.push(database.tables['post'].create({ id: posts.length + 1, authorId: 2, content: 'post1' }))
      posts.push(database.tables['post'].create({ id: posts.length + 1, authorId: 2, content: 'post2' }))

      await database.set('user', 2, {
        posts: $.update<Post>({
          $create: [
            { content: 'post1' },
            { content: 'post2' },
          ],
        }),
      })
      await expect(database.get('post', {})).to.eventually.have.deep.members(posts)

      posts.push(database.tables['post'].create({ id: 101, authorId: 1, content: 'post101' }))
      await database.set('user', 1, row => ({
        value: $.add(row.id, 98),
        posts: $.update({
          $create: { id: 101, content: 'post101' },
        }),
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
        posts: $.update({
          $set: r => ({
            score: $.add(row.id, r.id),
          }),
        }),
      }))
      await expect(database.get('post', {})).to.eventually.have.deep.members(posts)

      posts[1].score = 13
      await database.set('user', 1, row => ({
        posts: $.update({
          $set: [
            { score: { $gt: 2 } },
            r => ({ score: $.add(r.score, 10) }),
          ],
        }),
      }))
      await expect(database.get('post', {})).to.eventually.have.deep.members(posts)
    })

    it('delete oneToMany', async () => {
      await setup(database, 'user', userTable)
      await setup(database, 'profile', profileTable)
      const posts = await setup(database, 'post', postTable)

      posts.splice(0, 1)
      await database.set('user', {}, row => ({
        posts: $.update({
          $remove: r => $.eq(r.id, row.id),
        }),
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
    })

    it('connect / disconnect manyToMany', async () => {
      await setup(database, 'user', userTable)
      await setup(database, 'profile', profileTable)
      await setup(database, 'post', postTable)
      await setup(database, 'tag', tagTable)
      await setup(database, 'post2tag', post2TagTable)
      await setup(database, Relation.buildAssociationTable('post', 'tag') as any, post2TagTable.map(x => ({
        post_id: x.postId,
        tag_id: x.tagId,
      })))

      await database.set('post', 2, {
        tags: $.update({
          $disconnect: {},
        }),
      })
      await expect(database.get('post', 2, ['tags'])).to.eventually.have.nested.property('[0].tags').deep.equal([])

      await database.set('post', 2, row => ({
        tags: $.update({
          $connect: r => $.eq(r.id, row.id),
        }),
      }))
      await expect(database.get('post', 2, ['tags'])).to.eventually.have.nested.property('[0].tags').with.shape([{
        id: 2,
      }])
    })
  }
}

export default RelationTests
