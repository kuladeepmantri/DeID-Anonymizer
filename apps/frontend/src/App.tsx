import React, { useState } from 'react'
import { createTheme, ThemeProvider, CssBaseline, Container, Box, Typography, Button, useMediaQuery, Paper, CircularProgress } from '@mui/material'
import axios from 'axios'

const App: React.FC = () => {
  const prefersDark = useMediaQuery('(prefers-color-scheme: dark)')
  const theme = React.useMemo(() => createTheme({ palette: { mode: prefersDark ? 'dark' : 'light' } }), [prefersDark])

  const [file, setFile] = useState<File | null>(null)
  const [uploading, setUploading] = useState(false)
  const [text, setText] = useState('Sample: john.doe@example.com, +1 415 555 1212')
  const [result, setResult] = useState('')

  const onDrop = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault()
    const f = e.dataTransfer.files?.[0]
    if (f) setFile(f)
  }

  const upload = async () => {
    if (!file) return
    setUploading(true)
    try {
      const form = new FormData()
      form.append('file', file)
      const res = await axios.post('/api/files/upload', form)
      alert('Uploaded: ' + res.data.key)
    } finally {
      setUploading(false)
    }
  }

  const runDeid = async () => {
    const res = await axios.post('/api/deid/text', { text })
    setResult(res.data.result)
  }

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Container maxWidth="md">
        <Box sx={{ py: 4 }}>
          <Typography variant="h4" gutterBottom>De-identification Platform</Typography>
          <Paper onDragOver={(e)=>e.preventDefault()} onDrop={onDrop} sx={{ p:3, textAlign:'center', border:'2px dashed', mb:2 }}>
            <Typography>Drag & drop a file here</Typography>
            {file && <Typography sx={{ mt:1 }}>Selected: {file.name}</Typography>}
            <Button variant="contained" sx={{ mt:2 }} disabled={!file || uploading} onClick={upload}>{uploading ? <CircularProgress size={20}/> : 'Upload'}</Button>
          </Paper>
          <Paper sx={{ p:3 }}>
            <Typography variant="h6">De-identify Text</Typography>
            <textarea value={text} onChange={(e)=>setText(e.target.value)} style={{ width:'100%', minHeight:120, marginTop:8 }} />
            <Button variant="contained" sx={{ mt:2 }} onClick={runDeid}>Run</Button>
            {result && <Typography sx={{ mt:2 }}>Result: {result}</Typography>}
          </Paper>
        </Box>
      </Container>
    </ThemeProvider>
  )
}

export default App