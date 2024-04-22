import { $, Database, Keys, Relation } from 'minato'
import { expect } from 'chai'
import { setup } from './utils'
import { isNullable } from 'cosmokit'

interface User {
  id: number
  value?: number
  profile?: Relation<Profile>
  posts?: Relation<Post[]>
}

interface Profile {
  id: number
  name?: string
  userId: number
  user?: Relation<User>
}

interface Post {
  id: number
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
    profile: 'expr',
    posts: 'expr',
  }, {
    autoInc: true,
    relation: {
      profile: {
        type: 'oneToOne',
        table: 'profile',
        fields: ['id'],
        references: ['userId'],
      },
      posts: {
        type: 'oneToMany',
        table: 'post',
        fields: ['id'],
        references: ['authorId'],
      }
    }
  })

  database.extend('profile', {
    id: 'unsigned',
    name: 'string',
    userId: 'unsigned',
    user: 'expr',
  }, {
    autoInc: true,
    relation: {
      user: {
        type: 'oneToOne',
        table: 'user',
        fields: ['userId'],
        references: ['id'],
      },
    }
  })

  database.extend('post', {
    id: 'unsigned',
    content: 'string',
    authorId: 'unsigned',
    author: 'expr',
    tags: 'expr',
    _tags: 'expr',
  }, {
    autoInc: true,
    relation: {
      author: {
        type: 'manyToOne',
        table: 'user',
        fields: ['authorId'],
        references: ['id'],
      },
      tags: {
        type: 'manyToMany',
        table: 'tag',
        fields: ['id'],
        references: ['id'],
      },
      _tags: {
        type: 'oneToMany',
        table: 'post2tag',
        fields: ['id'],
        references: ['postId'],
      },
    }
  })

  database.extend('tag', {
    id: 'unsigned',
    name: 'string',
    posts: 'expr',
    _posts: 'expr',
  }, {
    autoInc: true,
    relation: {
      posts: {
        type: 'manyToMany',
        table: 'post',
        fields: ['id'],
        references: ['id'],
      },
      _posts: {
        type: 'oneToMany',
        table: 'post2tag',
        fields: ['id'],
        references: ['tagId'],
      },
    }
  })

  database.extend('post2tag', {
    id: 'unsigned',
    postId: 'unsigned',
    tagId: 'unsigned',
    post: 'expr',
    tag: 'expr',
  }, {
    autoInc: true,
    // primary: ['postId', 'tagId'],
    relation: {
      post: {
        type: 'manyToOne',
        table: 'post',
        fields: ['postId'],
        references: ['id'],
      },
      tag: {
        type: 'manyToOne',
        table: 'tag',
        fields: ['tagId'],
        references: ['id'],
      },
    }
  })
}

namespace RelationTests {
  const userTable: User[] = [
    { id: 1, value: 0 },
    { id: 2, value: 1 },
    { id: 3, value: 2 },
  ]

  const profileTable: Profile[] = [
    { id: 1, userId: 1, name: 'Apple' },
    { id: 2, userId: 2, name: 'Banana' },
    { id: 3, userId: 3, name: 'Cat' },
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


  function nm(l: any) {
    if (Array.isArray(l)) return l.length ? l : [{}]
    else if (isNullable(l)) return {}
    else return l
  }

  export function query(database: Database<Tables>) {
    it('basic support', async () => {
      const users = await setup(database, 'user', userTable)
      const profiles = await setup(database, 'profile', profileTable)
      const posts = await setup(database, 'post', postTable)

      await expect(database.select('profile', {}, { user: true }).execute()).to.eventually.have.shape(
        profiles.map(profile => ({
          ...profile,
          user: nm(users.find(user => user.id === profile.userId)),
        }))
      )

      await expect(database.select('user', {}, { profile: true, posts: true }).execute()).to.eventually.have.shape(
        users.map(user => ({
          ...user,
          profile: nm(profiles.find(profile => profile.userId === user.id)),
          posts: nm(posts.filter(post => post.authorId === user.id)),
        }))
      )

      await expect(database.select('post', {}, { author: true }).execute()).to.eventually.have.shape(
        posts.map(post => ({
          ...post,
          author: nm(users.find(user => user.id === post.authorId)),
        }))
      )
    })

    it('nested reads', async () => {
      const users = await setup(database, 'user', userTable)
      const profiles = await setup(database, 'profile', profileTable)
      const posts = await setup(database, 'post', postTable)

      await expect(database.select('user', {}, { posts: { author: true } }).execute()).to.eventually.have.shape(
        users.map(user => ({
          ...user,
          posts: nm(posts.filter(post => post.authorId === user.id).map(post => ({
            ...post,
            author: nm(users.find(user => user.id === post.authorId)),
          }))),
        }))
      )

      await expect(database.select('profile', {}, { user: { posts: { author: true } } }).execute()).to.eventually.have.shape(
        profiles.map(profile => ({
          ...profile, user: {
            ...nm(users.find(user => user.id === profile.userId)),
            posts: nm(posts.filter(post => post.authorId === profile.userId).map(post => ({
              ...post,
              author: nm(users.find(user => user.id === profile.userId)),
            }))),
          }
        }))
      )

      await expect(database.select('post', {}, { author: { profile: true } }).execute()).to.eventually.have.shape(
        posts.map(post => ({
          ...post, author: {
            ...nm(users.find(user => user.id === post.authorId)),
            profile: nm(profiles.find(profile => profile.userId === post.authorId)),
          }
        }))
      )
    })

    it('manyToMany', async () => {
      const users = await setup(database, 'user', userTable)
      const profiles = await setup(database, 'profile', profileTable)
      const posts = await setup(database, 'post', postTable)
      const tags = await setup(database, 'tag', tagTable)
      const post2tags = await setup(database, 'post2tag', post2TagTable)
      const re = await setup(database, 'post__tag__relation' as any, post2TagTable.map(x => ({
        post_id: x.postId,
        tag_id: x.tagId,
      })))
      // explicit manyToMany
      // console.dir(await database.select('post', {}, { _tags: { tag: { _posts: { post: true } } } }).execute(), { depth: 10 })
      // console.dir(await database.select('post', {}, { tags: { posts: true } }).execute(), { depth: 10 })

      await expect(database.select('post', {}, { tags: { posts: true } }).execute()).to.eventually.have.shape(
        posts.map(post => ({
          ...post,
          tags: {
            ...nm(post2tags.filter(p2t => p2t.postId === post.id)
              .map(p2t => tags.find(tag => tag.id === p2t.tagId))
              .filter(tag => tag)
              .map(tag => ({
                ...tag,
                posts: nm(post2tags.filter(p2t => p2t.tagId === tag!.id).map(p2t => posts.find(post => post.id === p2t.postId)))
              }))
            ),
          },
        }))
      )
    })
  }

  export function create(database: Database<Tables>) {
    it('basic support', async () => {
      const users = await setup(database, 'user', userTable)
      const profiles = await setup(database, 'profile', profileTable)
      const posts = await setup(database, 'post', postTable)

      await expect(database.select('profile', {}, { user: true }).execute()).to.eventually.have.shape(
        profiles.map(x => ({
          ...x, user: nm(users.find(p => p.id === x.id))
        }))
      )
    })
  }
}

export default RelationTests
