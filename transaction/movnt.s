// +build amd64

// Move 256 bytes. This involves 16 16-byte copy
TEXT ·movnt4x64b(SB), $0
    // Bytes 0 to 15
    MOVQ y+8(FP), AX // AX = src
    // movdqu %(rax), %xmm0 OR __m128i xmm0 = _mm_loadu_si128((__m128i *)src);
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    //movntdq %xmm0, (%rbx) OR _mm_stream_si128((__m128i *)dest, xmm0);
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 16 to 31
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 32 to 47
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 48 to 63
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 64 to 79
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 80 to 95
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 96 to 111
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 112 to 127
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 128 to 143
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 144 to 159
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 160 to 175
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 176 to 191
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 192 to 207
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 208 to 223
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 224 to 239
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 240 to 255
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    RET

// Move 128 bytes. This involves 8 16-byte copy
TEXT ·movnt2x64b(SB), $0
    // Bytes 0 to 15
    MOVQ y+8(FP), AX // AX = src
    // movdqu %(rax), %xmm0 OR __m128i xmm0 = _mm_loadu_si128((__m128i *)src);
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    //movntdq %xmm0, (%rbx) OR _mm_stream_si128((__m128i *)dest, xmm0);
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 16 to 31
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 32 to 47
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 48 to 63
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 64 to 79
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 80 to 95
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 96 to 111
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 112 to 127
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    RET

// Move 64 bytes. This involves 4 16-byte copy
TEXT ·movnt1x64b(SB), $0
    // Bytes 0 to 15
    MOVQ y+8(FP), AX // AX = src
    // movdqu %(rax), %xmm0 OR __m128i xmm0 = _mm_loadu_si128((__m128i *)src);
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    //movntdq %xmm0, (%rbx) OR _mm_stream_si128((__m128i *)dest, xmm0);
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 16 to 31
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 32 to 47
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 48 to 63
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    RET

// Move 32 bytes. This involves 2 16-byte copy
TEXT ·movnt1x32b(SB), $0
    // Bytes 0 to 15
    MOVQ y+8(FP), AX // AX = src
    // movdqu %(rax), %xmm0 OR __m128i xmm0 = _mm_loadu_si128((__m128i *)src);
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    //movntdq %xmm0, (%rbx) OR _mm_stream_si128((__m128i *)dest, xmm0);
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    // Bytes 16 to 31
    ADDQ $0x10, AX
    ADDQ $0x10, BX
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03

    RET

// Move 16 bytes
TEXT ·movnt1x16b(SB), $0
    MOVQ x+0(FP), BX // *dest
    MOVQ y+8(FP), AX // *src
    // movdqu %(rax), %xmm0 OR __m128i xmm0 = _mm_loadu_si128((__m128i *)src);
    BYTE $0xF3; BYTE $0x0F; BYTE $0x6F; BYTE $0x00
    //movntdq %xmm0, (%rbx) OR _mm_stream_si128((__m128i *)dest, xmm0);
    BYTE $0x66; BYTE $0x0f; BYTE $0xe7; BYTE $0x03
    RET

// Move 8 bytes
TEXT ·movnt1x8b(SB), $0
    MOVQ x+0(FP), BX // BX = dest
    MOVQ y+8(FP), AX // AX = src
    MOVQ (AX), DX // DX = *src
    MOVQ BX, AX // AX = dest
    // movnti %rdx, %(rax) // *dest = DX
    BYTE $0x48; BYTE $0x0f; BYTE $0xc3; BYTE $0x10
    RET

// Move 4 bytes
TEXT ·movnt1x4b(SB), $0
    MOVQ x+0(FP), BX // BX = dest
    MOVQ y+8(FP), AX // AX = src
    MOVL (AX), DX // DX = *src lower 32-b
    MOVQ BX, AX // AX = dest
    // movnti %edx, %(rax)
    BYTE $0x0f; BYTE $0xc3; BYTE $0x10
    RET
