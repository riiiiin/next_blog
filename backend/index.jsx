import useSWR, { unstable_serialize } from 'swr'

export async function getStaticProps () {
  const article = await getArticleFromAPI(1)
  return {
    props: {
      fallback: {
        // unstable_serialize() array style key
        [unstable_serialize(['api', 'article', 1])]: article,
      }
    }
  }
}

function Article() {
  // using an array style key.
  const { data } = useSWR(['api', 'article', 1], fetcher)
  return <h1>{data.title}</h1>
}

export default function Page({ fallback }) {
  return (
    <SWRConfig value={{ fallback }}>
      <Article />
    </SWRConfig>
  )
}