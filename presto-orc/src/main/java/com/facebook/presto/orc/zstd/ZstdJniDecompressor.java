/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.orc.zstd;

import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.OrcDecompressor;
import com.github.luben.zstd.Zstd;
import io.airlift.compress.MalformedInputException;
import io.airlift.compress.zstd.ZstdDecompressor;

import static java.lang.StrictMath.toIntExact;

public class ZstdJniDecompressor
        implements OrcDecompressor
{
    private final OrcDataSourceId orcDataSourceId;
    private final long maxBufferSize;

    public ZstdJniDecompressor(OrcDataSourceId orcDataSourceId, long maxBufferSize)
    {
        this.orcDataSourceId = orcDataSourceId;
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public int decompress(byte[] srcBytes, int offset, int length, OutputBuffer output)
            throws OrcCorruptionException
    {
        try {
            long uncompressedLength = ZstdDecompressor.getDecompressedSize(srcBytes, offset, length);
            if (uncompressedLength > this.maxBufferSize) {
                throw new OrcCorruptionException(orcDataSourceId, "Zstd requires buffer (%s) larger than max size (%s)", uncompressedLength, maxBufferSize);
            }
            byte[] dstBuffer = output.initialize(toIntExact(uncompressedLength));
            long size = Zstd.decompressByteArray(dstBuffer, 0, dstBuffer.length, srcBytes, offset, length);
            return (int) size;
        }
        catch (MalformedInputException e) {
            throw new OrcCorruptionException(e, orcDataSourceId, "Invalid compressed stream");
        }
    }
}
