const restler = require('restler')

const AWS = require('aws-sdk')
const s3 = new AWS.S3({ region: process.env.AWS_REGION })
const transcoder = new AWS.ElasticTranscoder({ region: process.env.AWS_REGION })

const stream = require('stream')
const binaryServer = require('binaryjs').BinaryServer
const wav = require('wav')

const server = binaryServer({ port: 9001 })

function uploaderFactory (streamId, token) {
  var pass = new stream.PassThrough()

  const key = `streams/${streamId}/audio.wav`
  var params = { Bucket: process.env.AWS_S3_BUCKET_NAME, Key: key, Body: pass }

  const options = { partSize: 5 * 1024 * 1024, queueSize: 3, computeChecksums: true }
  s3.upload(params, options, (err, data) => {
    if (!err) {
      doTranscode(key, streamId, JSON.parse(data.ETag), token)
    }
  })

  return pass
}

function doTranscode (inputKey, streamId, hash, token) {
  const newKey = `${hash}.mp3`

  var params = {
    PipelineId: process.env.AWS_ELASTIC_TRANSCODER_PIPELINE_ID,
    OutputKeyPrefix: `streams/${streamId}/`,
    Input: {
      Key: inputKey,
      FrameRate: 'auto',
      Resolution: 'auto',
      AspectRatio: 'auto',
      Interlaced: 'auto',
      Container: 'auto'
    },
    Outputs: [{
      Key: newKey,
      PresetId: process.env.AWS_ELASTIC_TRANSCODER_PRESET_ID
    }]
  }

  transcoder.createJob(params, (err, data) => {
    if (err) {
      console.error(err, err.stack)
      return
    }

    doUpdate(streamId, `streams/${streamId}/${newKey}`, token)
  })
}

function doUpdate (streamId, key, token) {
  const payload = {
    stream: {
      audio_s3_key: key
    }
  }

  restler.patchJson(
    `${process.env.STREAMR_API_ENDPOINT}/streams/${streamId}`,
    payload,
    { accessToken: token }
  )
    .on('success', (data, response) => console.log(data, response))
    .on('fail', (data, response) => console.log(data, response))
}

server.on('connection', (client) => {
  var uploader = null
  var wavWriter = null

  client.on('stream', (bitStream, meta) => {
    const streamId = meta.streamId

    uploader = uploader || uploaderFactory(streamId, meta.authToken)
    wavWriter = wavWriter || new wav.Writer({
      channels: 1,
      sampleRate: meta.sampleRate,
      bitDepth: 16
    })

    bitStream.pipe(wavWriter).pipe(uploader)
  })

  client.on('close', () => {
    if (wavWriter != null) {
      wavWriter.end()
      uploader.end()
    }
  })
})
