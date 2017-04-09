const AWS = require('aws-sdk')
const s3 = new AWS.S3({ region: process.env.AWS_REGION })
const transcoder = new AWS.ElasticTranscoder({ region: process.env.AWS_REGION })

const stream = require('stream')

const binaryServer = require('binaryjs').BinaryServer
const wav = require('wav')

const server = binaryServer({ port: 9001 })

function uploaderFactory (streamId) {
  var pass = new stream.PassThrough()

  const key = `streams/${streamId}/audio.wav`
  var params = { Bucket: process.env.AWS_S3_BUCKET_NAME, Key: key, Body: pass }

  const options = { partSize: 5 * 1024 * 1024, queueSize: 3, computeChecksums: true }
  s3.upload(params, options, (err, data) => {
    if (!err) {
      console.log('Uploaded.')
      doTranscode(key, streamId, JSON.parse(data.ETag))
    }
  })

  return pass
}

function doTranscode (inputKey, streamId, hash) {
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

  console.log('Beginning transcoding...', params)

  transcoder.createJob(params, (err, data) => {
    if (err) {
      console.log(err, err.stack)
      return
    }

    console.log('Done!', data)
  })
}

server.on('connection', (client) => {
  var uploader = null
  var wavWriter = null

  wavWriter = new wav.Writer({
    channels: 1,
    sampleRate: 48000,
    bitDepth: 16
  })

  client.on('stream', (bitStream, meta) => {
    const streamId = meta.streamId

    uploader = uploader || uploaderFactory(streamId)

    bitStream.pipe(wavWriter).pipe(uploader)
  })

  client.on('close', () => {
    if (wavWriter != null) {
      wavWriter.end()
      uploader.end()
    }
  })
})
